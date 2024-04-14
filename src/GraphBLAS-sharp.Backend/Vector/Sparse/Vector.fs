namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open Brahma.FSharp
open Microsoft.FSharp.Control
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVector
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Quotes

module Vector =
    let copy (clContext: ClContext) workGroupSize =
        let copy = ClArray.copy clContext workGroupSize

        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: DeviceCommandQueue<_>) allocationMode (vector: Sparse<'a>) ->
            { Context = clContext
              Indices = copy processor allocationMode vector.Indices
              Values = copyData processor allocationMode vector.Values
              Size = vector.Size }

    let copyTo (clContext: ClContext) workGroupSize =
        let copyTo = ClArray.copyTo clContext workGroupSize

        let copyDataTo = ClArray.copyTo clContext workGroupSize

        fun (processor: DeviceCommandQueue<_>) (source: Sparse<'a>) (destination: Sparse<'a>) ->
            copyTo processor source.Indices destination.Indices
            copyDataTo processor source.Values destination.Values

    let map = Map.run

    let mapWithValue = Map.WithValueOption.run

    let map2 = Map2.run

    let map2SparseDense = Map2.runSparseDense

    let map2AtLeastOne opAdd (clContext: ClContext) workGroupSize allocationMode =
        Map2.AtLeastOne.run (Convert.atLeastOneToOption opAdd) clContext workGroupSize allocationMode

    let assignByMask = Map2.assignByMask

    let toDense (clContext: ClContext) workGroupSize =

        let toDense =
            <@ fun (ndRange: Range1D) length (values: ClArray<'a>) (indices: ClArray<int>) (resultArray: ClArray<'a option>) ->
                let gid = ndRange.GlobalID0

                if gid < length then
                    let index = indices.[gid]

                    resultArray.[index] <- Some values.[gid] @>

        let kernel = clContext.Compile(toDense)

        let create =
            ClArray.zeroCreate clContext workGroupSize

        fun (processor: DeviceCommandQueue<_>) allocationMode (vector: ClVector.Sparse<'a>) ->
            let resultVector =
                create processor allocationMode vector.Size

            let ndRange =
                Range1D.CreateValid(vector.Indices.Length, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc ndRange vector.Indices.Length vector.Values vector.Indices resultVector)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            resultVector

    let reduce<'a when 'a: struct> (opAdd: Expr<'a -> 'a -> 'a>) (clContext: ClContext) workGroupSize =

        let reduce =
            Common.Reduce.reduce opAdd clContext workGroupSize

        fun (processor: DeviceCommandQueue<_>) (vector: ClVector.Sparse<'a>) -> reduce processor vector.Values

    let ofList (clContext: ClContext) allocationMode size (elements: (int * 'a) list) =
        let indices, values =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        { Context = clContext
          Indices = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, indices)
          Values = clContext.CreateClArrayWithSpecificAllocationMode(allocationMode, values)
          Size = size }
