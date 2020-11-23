namespace Hobbes.Web

open System.Security.Cryptography
open System.IO
open Hobbes.Web.Database

module Security =
    let inline private env name defaultValue = 
            match System.Environment.GetEnvironmentVariable name with
            null -> defaultValue
            | v -> v.Trim()
    [<Literal>]
    let private Basic = "basic "
    let private encoding = System.Text.Encoding.UTF8

    let private getBytes (s:string)= 
        encoding.GetBytes s

    let private toB64 s = 
        System.Convert.ToBase64String s

    let fromB64 s =  
        System.Convert.FromBase64String s
        |> encoding.GetString

    type private JwtPayload = FSharp.Data.JsonProvider<"""{"name":"some"}""">
    let private keySuffix = 
        env "KEY_SUFFIX" "development"

    let private getSignature personalKey header payload = 
        let hmac = System.Security.Cryptography.HMAC.Create("HMACSHA256")
        hmac.Key <- 
            personalKey + keySuffix
            |> getBytes

        payload
        |> getBytes
        |> toB64
        |> sprintf "%s.%s" header
        |> getBytes
        |> hmac.ComputeHash
        |> toB64

    let private cryptoKey = 
           let keySize = 32
           [|for i in 0..keySize - 1 -> keySuffix.[i % keySuffix.Length]|]
           |> System.String
           |> getBytes

    let private initializationVector = 
           [|for i in cryptoKey.Length..(cryptoKey.Length + 15) -> keySuffix.[i % keySuffix.Length]|]
           |> System.String
           |> getBytes

    let encrypt (plainText : string) = 
        use rijAlg = new RijndaelManaged()
        rijAlg.Key <- cryptoKey
        rijAlg.IV <- initializationVector
        let encryptor = rijAlg.CreateEncryptor(rijAlg.Key, rijAlg.IV)
        use msEncrypt = new MemoryStream()
        (
            use csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write)
            use swEncrypt = new StreamWriter(csEncrypt)
            swEncrypt.Write(plainText)
        )
        msEncrypt.Close()
        msEncrypt.ToArray()
        |> toB64

    let decrypt (base64Text : string) = 
        let cipherText = base64Text |> System.Convert.FromBase64String
        use rijAlg = new RijndaelManaged()
        rijAlg.Key <- cryptoKey
        rijAlg.IV <- initializationVector
        let decryptor = rijAlg.CreateDecryptor(rijAlg.Key, rijAlg.IV)
        use msDecrypt = new MemoryStream(cipherText)
        (
            use csDecrypt = new CryptoStream(msDecrypt, decryptor, CryptoStreamMode.Read)
            use srDecrypt = new StreamReader(csDecrypt)
            srDecrypt.ReadToEnd()
        )

    let createToken (user : UserRecord.Root) = 
        let header = 
            """{"alg":"HS256","type":"JWT"}"""
            |> getBytes
            |> toB64
        let payload = sprintf """{"name": "%s"}""" user.Name
        let signature = getSignature user.DerivedKey header payload
        sprintf "%s.%s.%s" header payload signature
        |> encrypt

    let private verifyKey (key : string) = 
        
            match key.Split('.') with
            [|header;payload;signature|] ->
                
                let jwtPayload = JwtPayload.Parse payload
                let user = users.Get (sprintf "org.couchdb.user:%s" jwtPayload.Name)
                let verified = signature = getSignature user.DerivedKey header payload
                if not verified then
                    eprintfn "Signatures didn't match"
                verified
            | _ -> 
                eprintfn "Tried to gain access with %s" key
                false


    type private AzureUser = FSharp.Data.JsonProvider<"""{"Email": " lkjljk", "User" : "lkjlkj"}""">

    let tryParseUser (user : string) = 
        match user.Split('|') with
        [|user;_;token|] ->
            let userInfo = 
                try
                    user
                    |> fromB64
                with e ->
                   eprintfn "Failed decoding token (%s). Message: %s" user e.Message
                   reraise()
            //github and azure use different formats so lets try and align them
            let userText = 
                userInfo.Replace("\"email\"", "\"Email\"").Replace("\"user\"","\"User\"").Trim().Replace(" ", ",").Trim().TrimStart('{').TrimEnd('}')
                |> sprintf "{%s}"
            let user = 
                userText
                |> AzureUser.Parse
            let userName = 
                if System.String.IsNullOrWhiteSpace(user.User) then
                    user.Email.Split('@', System.StringSplitOptions.RemoveEmptyEntries) |> Array.head
                else
                    user.User 
            Some(userName, token)
        | _ -> None

    let randomString length =
        let chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_"
        use crypto = new RNGCryptoServiceProvider()

        // Maximum random number that can be used without introducing a bias
        let maxRandom = 255 - (256 % chars.Length) |> byte
        let data = Array.create length 0uy
        let rec inner acc =
            crypto.GetBytes(data);

            let temp = 
                data
                |> Array.filter(fun v -> v <= maxRandom)
                |> List.ofArray
                
            match acc@temp with
            lst when lst.Length >= length ->
                let result = 
                    lst
                    |> List.take (min length data.Length)
                    |> List.map(fun v -> chars.[int v % chars.Length])
                    |> Array.ofList
                System.String(result)
            | l -> inner l
        inner []

    let verifyAuthToken (authToken : string) = 
        try

            let key = 
                printfn "Auth token: (%s)" authToken
                if authToken.ToLower().StartsWith(Basic) then
                        authToken.Substring(Basic.Length)
                        |> fromB64
                        |> (fun s -> 
                            s.Split(":").[0] //Ignore password part of basic
                           )
                else
                    authToken

            if (env "MASTER_USER" null) = key then 
                printfn "Authenticating using master key"
                true
            else
                key 
                |> decrypt
                |> verifyKey
        with e ->
            eprintfn "verification error. %s" e.Message
            false