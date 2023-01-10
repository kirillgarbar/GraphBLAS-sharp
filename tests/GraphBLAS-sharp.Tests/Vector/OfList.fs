module Backend.Vector.OfList

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend
open Context
open TestCases
let logger = Log.create "Vector.ofList.Tests"

let checkResult
    (isEqual: 'a -> 'a -> bool)
    (expectedIndices: int [])
    (expectedValues: 'a [])
    (actual: Vector<'a>)
    actualSize
    =

    Expect.equal actual.Size actualSize "lengths must be the same"

    match actual with
    | VectorSparse actual ->
        compareArrays (=) actual.Indices expectedIndices "indices must be the same"
        compareArrays isEqual actual.Values expectedValues "values must be the same"
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest<'a when 'a: struct>
    (isEqual: 'a -> 'a -> bool)
    (ofList: VectorFormat -> int -> (int * 'a) list -> ClVector<'a>)
    (toCoo: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (elements: (int * 'a) [])
    (sizeDelta: int)
    =

    let elements =
        elements |> Array.distinctBy fst |> List.ofArray

    if elements.Length > 0 then

        let q = case.ClContext.Queue

        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let actualSize = (Array.max indices) + abs sizeDelta + 1

        let clActual = ofList case.Format actualSize elements

        let clCooActual = toCoo q clActual

        let actual = clCooActual.ToHost q

        clActual.Dispose q
        clCooActual.Dispose q

        checkResult isEqual indices values actual actualSize

let testFixtures (case: OperationCase<VectorFormat>) =
    [ let config = defaultConfig

      let wgSize = 32

      let context = case.ClContext.ClContext
      let q = case.ClContext.Queue

      q.Error.Add(fun e -> failwithf $"%A{e}")

      let getCorrectnessTestName datatype =
          sprintf "Correctness on %s, %A" datatype case.Format

      let boolOfList = Vector.ofList context

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<bool> (=) boolOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let intOfList = Vector.ofList context

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<int> (=) intOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int")


      let byteOfList = Vector.ofList context

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<byte> (=) byteOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let floatOfList = Vector.ofList context

      let toCoo = Vector.toSparse context wgSize

      case
      |> correctnessGenericTest<byte> (=) floatOfList toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float") ]

let tests =
    operationGPUTests "Backend.Vector.ofList tests" testFixtures