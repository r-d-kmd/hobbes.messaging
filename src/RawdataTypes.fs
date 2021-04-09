namespace Hobbes.Web

open FSharp.Data

module RawdataTypes =
    type Transformation = 
        {
            [<Newtonsoft.Json.JsonProperty("_id")>]
            Name : string
            Statements : string list
            Description : string
        }
    
    type Config = JsonProvider<"""[{
            "_id": "hkjh",
            "sourceHash": "lkjlkjlkjlkj",
            "meta" : {
               "project" : "lkjlkj"
            },
            "source": {
                "provider": "odata",
                "url": "https://analytics.dev.azure.com/kmddk/flowerpot/_odata/v2.0/WorkItemRevisions?",
                "expand": "Iteration",
                "select": "WorkItemId,WorkItemType,Iteration,CycleTimeDays,ChangedDate",
                "filter": "Iteration%2FStartDate%20gt%202019-01-01Z",
                "user": "a6plj3mmef7ithp54265tqr2kxeu2huz4s2hemgtudkh2sd2vyhq",
                "pwd": "a6plj3mmef7ithp54265tqr2kxeu2huz4s2hemgtudkh2sd2vyhq"
            },
            "transformation" : "kjlkjlkjlkjlkjlkj"
        }, {
            "_id" : "name",
            "sourceHash": "lkjlkjlkjlkj",
            "meta" : {
               "project" : "lkjlkj",
               "source" : "git-azure-devops"
            },
            "source" : {
                "provider": "rest",
                "urls": [
                    "https://dev.azure.com/kmddk/kmdlogic/_apis/git/repositories/01c03de4-5713-4cad-b3d6-ff14dc4c387e/commits?api-version=6.0&$top=10000",
                    "https://dev.azure.com/kmddk/kmdlogic/_apis/git/repositories/8622dba3-3a68-4a16-964a-03c42fd6033a/commits?api-version=6.0&$top=10000"
                ],
                "user": "a6plj3mmef7ithp54265tqr2kxeu2huz4s2hemgtudkh2sd2vyhq", 
                "pwd": "a6plj3mmef7ithp54265tqr2kxeu2huz4s2hemgtudkh2sd2vyhq",
                "values": "value"
            },
            "transformation" : "jlk"
        }]""", SampleIsList = true>
        
    let keyFromSourceDoc (source : string) = 
        source
        |> Hobbes.Web.Cache.key
        
    let keyFromSource (source : Config.Source) = 
        source.JsonValue.ToString()
        |> keyFromSourceDoc
    
    let keyFromConfig (config : Config.Root) =
        try 
                let source = config.Source
                let sourceId =  source |> keyFromSource
                System.String.Join(":",[sourceId;config.Transformation |> Hobbes.Helpers.Environment.hash])
                
        with e ->
           failwithf "Failed to get key from (%s). Message: %s. Trace: %s" (config.JsonValue.ToString()) e.Message e.StackTrace
    
    let keyFromConfigDoc = 
        Config.Parse
        >> keyFromConfig