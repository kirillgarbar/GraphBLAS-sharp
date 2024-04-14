namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp

module ArraysExtensions =
    type ClArray<'a> with
        member this.FreeAndWait(q: DeviceCommandQueue<Msg>) =
            q.Post(Msg.CreateFreeMsg this)
            q.Synchronize()

        member this.ToHost(q: DeviceCommandQueue<Msg>) =
            let dst = Array.zeroCreate this.Length
            q.Post(Msg.CreateToHostMsg(this, dst))
            q.Synchronize()
            dst

        member this.Free(q: DeviceCommandQueue<_>) = q.Post <| Msg.CreateFreeMsg this

        member this.ToHostAndFree(q: DeviceCommandQueue<_>) =
            let result = this.ToHost q
            this.Free q

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
