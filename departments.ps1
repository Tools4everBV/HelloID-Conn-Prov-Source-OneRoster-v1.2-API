[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
Write-Information "Processing Departments"
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
                $response = Invoke-RestMethod @splat -Proxy "http://localhost:8888"
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
    } catch {
        throw "Get Data Failed - $($_)"
    }
#endregion Get Data

#region Return Data
$return = [System.Collections.Generic.List[psobject]]::new()
foreach($org in $orgs)
{
    $department = @{
        ExternalId  = $org.sourcedId
        DisplayName = $org.name
        Name        = $org.Name
        Identifier  = $org.identifier
        Type        = $org.type
    }

    Write-Output ($department | ConvertTo-Json -Depth 10)
}
Write-Information "Finished Processing Departments"
#endregion Return Data
