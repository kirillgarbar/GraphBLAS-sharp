module Backend.RemoveDuplicates

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common

let logger = Log.create "RemoveDuplicates.Tests"

let testCases =
    [ testCase "Simple correctness test"
      <| fun () ->
          let array = [| 1; 2; 2; 3; 3; 3 |]

          let actual =
              opencl {
                  let! copiedArray = Copy.copyArray array
                  let! result = RemoveDuplicates.fromArray copiedArray

                  if array.Length <> 0 then
                      failwith "fix me"
                      //let! _ = ToHost result
                      ()

                  return result
              }
              //|> OpenCLEvaluationContext().RunSync
              failwith "fix me"

          logger.debug (
              eventX "Actual is {actual}"
              >> setField "actual" (sprintf "%A" actual)
          )

          let expected = [| 1; 2; 3 |]

          "Array should be without duplicates"
          |> Expect.sequenceEqual actual expected

      testCase "Test on empty array"
      <| fun () ->
          let array = Array.zeroCreate<int> 0

          let actual =
              opencl {
                  let! copiedArray = Copy.copyArray array
                  let! result = RemoveDuplicates.fromArray copiedArray

                  if array.Length <> 0 then
                      failwith "fix me"
                      //let! _ = ToHost result
                      ()

                  return result
              }
              //|> OpenCLEvaluationContext().RunSync
              failwith "fix me"

          logger.debug (
              eventX "Actual is {actual}"
              >> setField "actual" (sprintf "%A" actual)
          )

          let expected = array

          "Array should be without duplicates"
          |> Expect.sequenceEqual actual expected ]

let tests =
    testCases |> ptestList "RemoveDuplicates tests"
