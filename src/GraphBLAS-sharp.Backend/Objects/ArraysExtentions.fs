namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp

module ArraysExtensions =
    type ClArray<'a> with
        member this.FreeAndWait(q: RawCommandQueue) =
            this.Dispose()
            q.Synchronize()

        member this.ToHost(q: RawCommandQueue) =
            let dst = Array.zeroCreate this.Length
            q.ToHost(this, dst, true)
            dst

        member this.Free() = this.Dispose()

        member this.ToHostAndFree(q: RawCommandQueue) =
            let result = this.ToHost q
            this.Free()

            result

        member this.Size = this.Length

    type 'a ``[]`` with
        member this.Size = this.Length

        member this.ToDevice(context: ClContext) = context.CreateClArray this

    let DenseVectorToString (array: 'a []) =
        [ sprintf "Dense Vector\n"
          sprintf "Size:    %i \n" array.Length
          sprintf "Values:  %A \n" array ]
        |> String.concat ""

    let DenseVectorFromArray (array: 'a [], isZero: 'a -> bool) =
        array
        |> Array.map (fun v -> if isZero v then None else Some v)
