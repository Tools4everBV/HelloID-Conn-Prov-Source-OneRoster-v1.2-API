[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
Write-Information "Processing Persons"
#region Configuration
$config = ConvertFrom-Json $configuration
#endregion Configuration

#region Support Functions
function Get-AuthToken {
    [cmdletbinding()]
    Param (
        [string]$BaseUri, 
        [string]$TokenUri, 
        [string]$ClientKey,
        [string]$ClientSecret,
        [string]$PageSize
    ) 
    Process 
    {
        $requestUri = $TokenURI
        
        $pair       = "{0}:{1}" -f $ClientKey,$ClientSecret
        $bytes      = [System.Text.Encoding]::ASCII.GetBytes($pair)
        $bear_token = [System.Convert]::ToBase64String($bytes)
        $headers    = @{   
            Authorization = "Basic {0}" -f $bear_token
            Accept  = "application/json"
        }
        
        $parameters = @{
                        grant_type="client_credentials"
                        scope='http://purl.imsglobal.org/spec/or/v1p2/scope/roster.readonly http://purl.imsglobal.org/spec/or/v1p2/scope/roster-demographics.readonly http://purl.imsglobal.org/spec/or/v1p2/scope/resource.readonly https://purl.imsglobal.org/spec/or/v1p2/scope/gradebook.readonly https://purl.imsglobal.org/spec/or/v1p2/scope/gradebook-core.readonly https://purl.imsglobal.org/spec/or/v1p2/scope/gradebook.createput'
                    }
        
        Write-Information ("POST {0}" -f $requestUri)
        $splat = @{
            Method  = 'Post'
            URI     = $requestUri
            Body    = $parameters 
            Headers = $headers 
            Verbose = $false
        }
        $response = Invoke-RestMethod @splat
        #Write-Information $response
        $accessToken       = $response.access_token
    
        #Add the authorization header to the request
        $authorization = @{
            Authorization  = "Bearer {0}" -f $accesstoken
            'Content-Type' = "application/json"
            Accept         = "application/json"
        }
        $authorization
    }
}


function Get-Data {
    [cmdletbinding()]
    Param (
        [string]$BaseUri, 
        [string]$TokenUri, 
        [string]$ClientKey,
        [string]$ClientSecret,
        [string]$PageSize,
        [string]$EndpointUri,
        [string]$PropertyName,
        [string]$Filter,
        [object]$Authorization
    ) 
    Begin
    {
        $offset        = 0
        $requestUri    = "{0}{1}" -f $BaseURI,$EndPointUri

        $propertyArray = $PropertyName
        
        $results       = [System.Collections.Generic.List[object]]::new()
    }
    
    Process 
    {
        do
        {
            $parameters = [ordered]@{}

            if($filter -ne $null -and $filter.Length -gt 0)
            {
              $parameters['filter'] = $filter      
            }

            $parameters['limit'] =$Pagesize
            $parameters['offset'] = $offset
            
           
            Write-Information ("GET {0} ({1})" -f $requestUri, $offset)

            $splat = @{
                Method  = 'GET'
                Uri     = $requestUri 
                Body    = $parameters 
                Headers = $Authorization 
                Verbose = $false
            }
            
            try {
                $response = Invoke-RestMethod @splat
            }
            catch {
                if($_.Exception.Response.StatusCode.value__ -eq 401)
                {
                    throw "Client is unauthorized"
                }
                elseif($_.Exception.Response.StatusCode.value__ -eq 404)
                {
                    throw "Endpoint is not found (404) - $($requestUri)"
                }
                else
                {
                    Write-Warning ("  Retrying RestMethod.  Error:  {0}" -f $_)
                    Start-Sleep -seconds 5
                    $response = Invoke-RestMethod @splat
                }
            }

            if($response.$propertyArray.getType().BaseType -eq [System.Array])
            {
                $results.AddRange($response.$propertyArray)
            }
            else
            {
                $results.Add($response.$propertyArray)
            }
            
            $offset = $offset + $response.$propertyArray.count
        } while ($response.$propertyArray.count -eq $PageSize)
    }
    
    End
    {
        return $results
    }
}

function Group-ObjectHashtable
{
    param(
        [string[]] $Property
    )

    begin
    {   # create an empty hashtable
        $hashtable = @{}
    }

    process
    {   # create a key based on the submitted properties, and turn it into a string
        $key = $(foreach($prop in $Property) { $_.$prop }) -join ','
        
        # check to see if the key is present already
        if ($hashtable.ContainsKey($key) -eq $false)
        {   # add an empty list
            $hashtable[$key] = [Collections.Generic.List[psobject]]::new()
        }

        # add element to appropriate array list:
        $hashtable[$key].Add($_)
    }

    end
    {   # return the entire hashtable:
        $hashtable
    }
}

#endregion Support Functions

#region Get Data
$splat = @{
    BaseURI = $config.BaseURI
    TokenUri = $config.TokenUri
    ClientKey = $config.ClientKey
    ClientSecret = $config.ClientSecret
    PageSize = $config.PageSize
}

    try {
        $splat['Authorization'] = Get-AuthToken @splat
    } catch {
        throw "Authorization Failed - $($_)"
    }

    try {
    $orgs               = Get-Data @splat -EndpointUri "/ims/oneroster/rostering/v1p2/orgs" -PropertyName "orgs" -Filter $config.OrgFilter
    $orgs_ht            = $orgs | Group-ObjectHashtable 'sourcedId'
        $orgs_empty = @{}
        $orgs[0].PSObject.Properties.ForEach({$orgs_empty[$_.name -Replace '\W','_'] = ''})
    $academicSessions   = Get-Data @splat -EndpointUri "/ims/oneroster/rostering/v1p2/academicSessions" -PropertyName "academicSessions"
    $academicSessions_ht= $academicSessions | Group-ObjectHashtable 'sourcedId'

    $enrollments        = Get-Data @splat -EndpointUri "/ims/oneroster/rostering/v1p2/enrollments" -PropertyName "enrollments" -Filter $config.EnrollmentFilter
        $enrollments_ht = $enrollments | Group-Object -Property @{e={$_.user.sourcedID}} -AsString -AsHashTable
    $classes            = Get-Data @splat -EndpointUri "/ims/oneroster/rostering/v1p2/classes" -PropertyName "classes" -Filter $config.ClassFilter
        $classes_ht     = $classes | Group-ObjectHashtable 'sourcedId'
    $courses            = Get-Data @splat -EndpointUri "/ims/oneroster/rostering/v1p2/courses" -PropertyName "courses" -Filter $config.CourseFilter
        $courses_ht     = $courses | Group-ObjectHashtable 'sourcedId'  

    #User can be used instead if guardians or other roles are needed. Filtering by roles doesn't seem to be working, thus separate endpoints vs just users.
    #$users             = Get-Data @splat -EndpointUri "/ims/oneroster/rostering/v1p2/users" -PropertyName "users" -Filter $confg.UserFIlter
    $students           = Get-Data @splat -EndpointUri "/ims/oneroster/rostering/v1p2/students" -PropertyName "users" -Filter $config.UserFilter
    $teachers           = Get-Data @splat -EndpointUri "/ims/oneroster/rostering/v1p2/teachers" -PropertyName "users" -Filter $config.UserFilter

    $demographics           = Get-Data @splat -EndpointUri "/ims/oneroster/rostering/v1p2/demographics" -PropertyName "demographics" -Filter $config.DemographicFilter
        $demographics_ht     = $demographics | Group-ObjectHashtable 'sourcedId'  

    $availablePersons = [System.Collections.Generic.List[object]]::new()
    #$availablePersons.AddRange($users)
    $availablePersons.AddRange($students)
    $availablePersons.AddRange($teachers)
    } catch {
        throw "Get Data Failed - $($_)"
    }
#endregion Get Data

#region Prepare Return Data
foreach($user in $availablePersons)
{  

    
    $person = @{}
    $person['ExternalId']   = '{0}' -f $user.sourcedId
    $person['DisplayName']  = '{0} {1} ({2})' -f $user.givenName, $user.familyName, $user.sourcedId
    
    $_skipfields = @("agents","grades")
    foreach($prop in ($user.PSObject.properties)) 
    {
        if($_skipfields -notcontains $prop.Name)
        {
            $person[$prop.Name -replace '\W','_'] = $prop.Value
        }
    }

    $person['demographics'] = try{ $demographics_ht[$user.sourcedId] } catch{''}

    # Grade - Convert from Array to just a string.
    $person['grades'] = try{$user.grades[0]}catch{''}

    # Not including Agents.  Only needed if mapping Parent/Guardian data.
    #$person['agents'] = $user.agents.sourcedId

    # Add Contracts
    $person['Contracts'] = [System.Collections.Generic.List[psobject]]::new()
    
    # Add Class Enrollments
    foreach($e in $enrollments_ht[$user.sourcedId.ToString()])
    {
        $contract = @{
            externalID = $e.sourcedId
            Class = @{}
        }
        # Process Enrollment Fields
        $_skipfields = @("class","school","user")
        foreach($prop in ($e.PSObject.properties)) # | ? {$_skipfields -notcontains $_.Name}))
        {
            if($_skipfields -notcontains $prop.Name)
            {
                $contract[$prop.Name -replace '\W','_'] = $prop.Value
            }
        }

        #Class for Enrollment
        $c = $classes_ht[$e.class.sourcedId.ToString()][0]
        $_skipfields = @("course","school","terms")  #"periods","subjects","subjectCodes",
        foreach($prop in ($c.PSObject.properties))# | ? {$_skipfields -notcontains $_.Name}))
        {
            if($_skipfields -notcontains $prop.Name)
            {
                $contract.class[$prop.Name -replace '\W','_'] = $prop.Value
            }
        }
            
        # Sequence used for Priority Logic.  Priority:  HomeRoom, scheduled, everything else
        switch ($c.classType)
        {
            'homeroom'  {$contract['Sequence'] = 1}
            'scheduled' {$contract['Sequence'] = 2}
            default     {$contract['Sequence'] = 3}
        }
        # Extra logic to lower priority of 'tobedeleted' records.
        if($contract.status -ne 'active') {$contract.Sequence = 3}

        #Academic Sessions/Terms for Class  (Not including Terms due to excessive memory use in HelloID error)
        #$contract['terms'] = [System.Collections.Generic.List[psobject]]::new()   
        foreach($_term in $c.terms)
        {
            $term = @{}
            $as = $academicSessions_ht[$_term.sourcedId.ToString()][0]
            $_skipfields = @("children","parent")
            foreach($prop in ($as.PSObject.properties)) # | ? {$_skipfields -notcontains $_.Name}))
            {
                if($_skipfields -notcontains $prop.Name)
                {
                    $term[$prop.Name -replace '\W','_'] = $prop.Value
                }
            }
            #$contract['terms'].Add($term)
            # Update Earliest and Latest Term Start/End Dates for Class.
            $contract['startDate'] = $(if(!$contract['startDate'] -OR $contract['startDate'] -gt $term.startDate){$term.startDate}else{$contract['startDate']})
            $contract['endDate'] = $(if(!$contract['endDate'] -OR $contract['endDate'] -lt $term.endDate){$term.endDate}else{$contract['endDate']})
        }
        #Course for Class
        $contract['course'] = @{}
        $crs = $courses_ht[$c.course.sourcedId.ToString()][0]
        $_skipfields = @("org","subjectCodes","subjects")
        foreach($prop in ($crs.PSObject.properties))  # | ? {$_skipfields -notcontains $_.Name}))
        {
            if($_skipfields -notcontains $prop.Name)
            {
                $contract.course[$prop.Name -replace '\W','_'] = $prop.Value
            }
        }

        #School for Enrollment
        $contract['school'] = @{}
        $sch = $orgs_ht[$c.school.sourcedId.ToString()][0]
        $_skipfields = @("parent")
        foreach($prop in ($sch.PSObject.properties)) # | ? {$_skipfields -notcontains $_.Name}))
        {
            if($_skipfields -notcontains $prop.Name)
            {
                $contract.school[$prop.Name -replace '\W','_'] = $prop.Value
            }
        }

        # Add Location Enrichment Data Here (if needed)

        $person.Contracts.Add($contract)
    }
    Write-Output ($person | ConvertTo-Json -Depth 10)
}
#endregion Prepare Return Data

#region Return Data to HelloID
Write-Information "Finished Processing Persons"
#endregion Return Data to HelloID
