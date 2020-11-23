namespace Hobbes.Helpers

[<AutoOpen>]
module Environment = 
    let env name defaultValue = 
            match System.Environment.GetEnvironmentVariable name with
            null -> defaultValue
            | v -> v.Trim()
    
    let hash (input : string) =
        use md5Hash = System.Security.Cryptography.MD5.Create()
        let data = md5Hash.ComputeHash(System.Text.Encoding.UTF8.GetBytes(input))
        let sBuilder = System.Text.StringBuilder()
        (data
        |> Seq.fold(fun (sBuilder : System.Text.StringBuilder) d ->
                sBuilder.Append(d.ToString("x2"))
        ) sBuilder).ToString()  
    