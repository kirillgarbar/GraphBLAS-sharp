namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp
open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions

module Scan =

    let private runInPlace3 (opAdd: Expr<'a -> 'a -> 'a>) (zero: 'a) (clContext: ClContext) (workGroupSize: int) =

        let blockSize = min clContext.ClDevice.MaxWorkGroupSize 256
        let valuesPerBlock = 2 * blockSize
        let numberOfMemBanks = 32
        let localArraySize = valuesPerBlock + (valuesPerBlock / numberOfMemBanks)

        let getIndex =
            <@ fun index -> index + (index / numberOfMemBanks) @>

        let preScan =
            <@ fun (ndRange: Range1D) (valuesLength: int) (valuesBuffer: ClArray<'a>) (carryBuffer: ClArray<'a>) ->
                let gid = ndRange.GlobalID0 / blockSize
                let lid = ndRange.LocalID0
                let gstart = gid * blockSize * 2

                let sumValues = localArray<'a> localArraySize

                //Load values
                if (gstart + lid + blockSize * 0) < valuesLength
                then
                    sumValues.[(%getIndex) (lid + blockSize * 0)] <- valuesBuffer.[gstart + lid + blockSize * 0]
                else
                    sumValues.[(%getIndex) (lid + blockSize * 0)] <- zero


                if (gstart + lid + blockSize * 1) < valuesLength
                then
                    sumValues.[(%getIndex) (lid + blockSize * 1)] <- valuesBuffer.[gstart + lid + blockSize * 1]
                else
                    sumValues.[(%getIndex) (lid + blockSize * 1)] <- zero

                //debug barrier
                // barrierLocal ()
                // if lid < valuesLength then printf "\n%i %i %i\n" lid (sumValues.[lid]) (valuesBuffer.[gstart + lid + blockSize * 0])

                //Sweep up
                let mutable offset = 1
                let mutable d = blockSize

                while d > 0 do
                    barrierLocal ()

                    if lid < d then
                        let ai = (%getIndex) (offset * (2 * lid + 1) - 1)
                        let bi = (%getIndex) (offset * (2 * lid + 2) - 1)
                        sumValues.[bi] <- (%opAdd) sumValues.[bi] sumValues.[ai]

                    offset <- offset * 2
                    d <- d / 2

                if lid = 0 then
                    let ai = (%getIndex) (2 * blockSize - 1)
                    carryBuffer.[gid] <- sumValues.[ai]
                    sumValues.[ai] <- zero

                //Sweep down
                d <- 1

                while d <= blockSize do
                    barrierLocal ()

                    offset <- offset / 2

                    if lid < d then
                        let ai = (%getIndex) (offset * (2 * lid + 1) - 1)
                        let bi = (%getIndex) (offset * (2 * lid + 2) - 1)

                        let tmp = sumValues.[ai]
                        sumValues.[ai] <- sumValues.[bi]
                        sumValues.[bi] <- (%opAdd) sumValues.[bi] tmp

                    d <- d * 2

                barrierLocal ()

                if (gstart + lid + blockSize * 0) < valuesLength then
                    valuesBuffer.[gstart + lid + blockSize * 0] <- sumValues.[(%getIndex) (lid + blockSize * 0)]
                if (gstart + lid + blockSize * 1) < valuesLength then
                    valuesBuffer.[gstart + lid + blockSize * 1] <- sumValues.[(%getIndex) (lid + blockSize * 1)] @>

        let preScan = clContext.Compile(preScan)

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let numberOfGroups = inputArray.Length / valuesPerBlock + (if inputArray.Length % valuesPerBlock = 0 then 0 else 1)

            if numberOfGroups > 1 then failwith "Number of groups at this point should be 1"

            let carry =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(
                    DeviceOnly,
                    numberOfGroups
                )

            let ndRangePreScan = Range1D.CreateValid(numberOfGroups * blockSize, blockSize)

            let preScanKernel = preScan.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> preScanKernel.KernelFunc ndRangePreScan inputArray.Length inputArray carry)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(preScanKernel))

            carry.Free processor

    let private runInPlace2 (opAdd: Expr<'a -> 'a -> 'a>) (zero: 'a) (clContext: ClContext) (workGroupSize: int) =

        let blockSize = min clContext.ClDevice.MaxWorkGroupSize 256
        let valuesPerBlock = 2 * blockSize
        let numberOfMemBanks = 32
        let localArraySize = valuesPerBlock + (valuesPerBlock / numberOfMemBanks)

        let getIndex =
            <@ fun index -> index + (index / numberOfMemBanks) @>

        let preScan =
            <@ fun (ndRange: Range1D) (valuesLength: int) (valuesBuffer: ClArray<'a>) (carryBuffer: ClArray<'a>) ->
                let gid = ndRange.GlobalID0 / blockSize
                let lid = ndRange.LocalID0
                let gstart = gid * blockSize * 2

                let sumValues = localArray<'a> localArraySize

                //Load values
                if (gstart + lid + blockSize * 0) < valuesLength
                then
                    sumValues.[(%getIndex) (lid + blockSize * 0)] <- valuesBuffer.[gstart + lid + blockSize * 0]
                else
                    sumValues.[(%getIndex) (lid + blockSize * 0)] <- zero


                if (gstart + lid + blockSize * 1) < valuesLength
                then
                    sumValues.[(%getIndex) (lid + blockSize * 1)] <- valuesBuffer.[gstart + lid + blockSize * 1]
                else
                    sumValues.[(%getIndex) (lid + blockSize * 1)] <- zero

                //debug barrier
                // barrierLocal ()
                // if lid < valuesLength then printf "\n%i %i %i\n" lid (sumValues.[lid]) (valuesBuffer.[gstart + lid + blockSize * 0])

                //Sweep up
                let mutable offset = 1
                let mutable d = blockSize

                while d > 0 do
                    barrierLocal ()

                    if lid < d then
                        let ai = (%getIndex) (offset * (2 * lid + 1) - 1)
                        let bi = (%getIndex) (offset * (2 * lid + 2) - 1)
                        sumValues.[bi] <- (%opAdd) sumValues.[bi] sumValues.[ai]

                    offset <- offset * 2
                    d <- d / 2

                if lid = 0 then
                    let ai = (%getIndex) (2 * blockSize - 1)
                    carryBuffer.[gid] <- sumValues.[ai]
                    sumValues.[ai] <- zero

                //Sweep down
                d <- 1

                while d <= blockSize do
                    barrierLocal ()

                    offset <- offset / 2

                    if lid < d then
                        let ai = (%getIndex) (offset * (2 * lid + 1) - 1)
                        let bi = (%getIndex) (offset * (2 * lid + 2) - 1)

                        let tmp = sumValues.[ai]
                        sumValues.[ai] <- sumValues.[bi]
                        sumValues.[bi] <- (%opAdd) sumValues.[bi] tmp

                    d <- d * 2

                barrierLocal ()

                if (gstart + lid + blockSize * 0) < valuesLength then
                    valuesBuffer.[gstart + lid + blockSize * 0] <- sumValues.[(%getIndex) (lid + blockSize * 0)]
                if (gstart + lid + blockSize * 1) < valuesLength then
                    valuesBuffer.[gstart + lid + blockSize * 1] <- sumValues.[(%getIndex) (lid + blockSize * 1)] @>

        let scan =
            <@ fun (ndRange: Range1D) (valuesLength: int) (valuesBuffer: ClArray<'a>) (carryBuffer: ClArray<'a>) ->
                let gid = ndRange.GlobalID0 + 2 * blockSize
                let cid = gid / (2 * blockSize)

                if gid < valuesLength then
                    valuesBuffer.[gid] <- (%opAdd) valuesBuffer.[gid] carryBuffer.[cid] @>

        let preScan = clContext.Compile(preScan)

        let scan = clContext.Compile(scan)

        let runInPlace3 = runInPlace3 opAdd zero clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let numberOfGroups = inputArray.Length / valuesPerBlock + (if inputArray.Length % valuesPerBlock = 0 then 0 else 1)

            let carry =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(
                    DeviceOnly,
                    numberOfGroups
                )

            let ndRangePreScan = Range1D.CreateValid(numberOfGroups * blockSize, blockSize)

            let preScanKernel = preScan.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> preScanKernel.KernelFunc ndRangePreScan inputArray.Length inputArray carry)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(preScanKernel))

            if numberOfGroups > 1 then
                runInPlace3 processor carry

                let ndRangeScan = Range1D.CreateValid((numberOfGroups - 1) * valuesPerBlock, blockSize)

                let scan = scan.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments(fun () -> scan.KernelFunc ndRangeScan inputArray.Length inputArray carry)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(scan))

            carry.Free processor

    let private runInPlace1 (opAdd: Expr<'a -> 'a -> 'a>) (zero: 'a) (clContext: ClContext) (workGroupSize: int) =

        let blockSize = min clContext.ClDevice.MaxWorkGroupSize 256
        let valuesPerBlock = 2 * blockSize
        let numberOfMemBanks = 32
        let localArraySize = valuesPerBlock + (valuesPerBlock / numberOfMemBanks)

        let getIndex =
            <@ fun index -> index + (index / numberOfMemBanks) @>

        let preScan =
            <@ fun (ndRange: Range1D) (valuesLength: int) (valuesBuffer: ClArray<'a>) (carryBuffer: ClArray<'a>) (totalSumCell: ClCell<'a>) ->
                let gid = ndRange.GlobalID0 / blockSize
                let lid = ndRange.LocalID0
                let gstart = gid * blockSize * 2

                let sumValues = localArray<'a> localArraySize

                //Load values
                if (gstart + lid + blockSize * 0) < valuesLength
                then
                    sumValues.[(%getIndex) (lid + blockSize * 0)] <- valuesBuffer.[gstart + lid + blockSize * 0]
                else
                    sumValues.[(%getIndex) (lid + blockSize * 0)] <- zero


                if (gstart + lid + blockSize * 1) < valuesLength
                then
                    sumValues.[(%getIndex) (lid + blockSize * 1)] <- valuesBuffer.[gstart + lid + blockSize * 1]
                else
                    sumValues.[(%getIndex) (lid + blockSize * 1)] <- zero

                //debug barrier
                // barrierLocal ()
                // if lid < valuesLength then printf "\n%i %i %i\n" lid (sumValues.[lid]) (valuesBuffer.[gstart + lid + blockSize * 0])

                //Sweep up
                let mutable offset = 1
                let mutable d = blockSize

                while d > 0 do
                    barrierLocal ()

                    if lid < d then
                        let ai = (%getIndex) (offset * (2 * lid + 1) - 1)
                        let bi = (%getIndex) (offset * (2 * lid + 2) - 1)
                        sumValues.[bi] <- (%opAdd) sumValues.[bi] sumValues.[ai]

                    offset <- offset * 2
                    d <- d / 2

                if lid = 0 then
                    let ai = (%getIndex) (2 * blockSize - 1)
                    carryBuffer.[gid] <- sumValues.[ai]
                    sumValues.[ai] <- zero
                    if gid = 0 then
                        //Save last for total sum
                        totalSumCell.Value <- valuesBuffer.[valuesLength - 1]

                //Sweep down
                d <- 1

                while d <= blockSize do
                    barrierLocal ()

                    offset <- offset / 2

                    if lid < d then
                        let ai = (%getIndex) (offset * (2 * lid + 1) - 1)
                        let bi = (%getIndex) (offset * (2 * lid + 2) - 1)

                        let tmp = sumValues.[ai]
                        sumValues.[ai] <- sumValues.[bi]
                        sumValues.[bi] <- (%opAdd) sumValues.[bi] tmp

                    d <- d * 2

                barrierLocal ()

                if (gstart + lid + blockSize * 0) < valuesLength then
                    valuesBuffer.[gstart + lid + blockSize * 0] <- sumValues.[(%getIndex) (lid + blockSize * 0)]
                if (gstart + lid + blockSize * 1) < valuesLength then
                    valuesBuffer.[gstart + lid + blockSize * 1] <- sumValues.[(%getIndex) (lid + blockSize * 1)] @>

        let scan =
            <@ fun (ndRange: Range1D) (valuesLength: int) (valuesBuffer: ClArray<'a>) (carryBuffer: ClArray<'a>) (totalSumCell: ClCell<'a>) ->
                let gid = ndRange.GlobalID0 + 2 * blockSize
                let cid = gid / (2 * blockSize)

                if gid < valuesLength then
                    valuesBuffer.[gid] <- (%opAdd) valuesBuffer.[gid] carryBuffer.[cid]
                    if gid = valuesLength - 1 then totalSumCell.Value <- (%opAdd) totalSumCell.Value valuesBuffer.[gid] @>

        let getTotalSum =
            <@ fun (ndRange: Range1D) (valuesLength: int) (valuesBuffer: ClArray<'a>) (totalSumCell: ClCell<'a>) ->
                totalSumCell.Value <- (%opAdd) valuesBuffer.[valuesLength - 1] totalSumCell.Value @>

        let preScan = clContext.Compile(preScan)
        let scan = clContext.Compile(scan)
        let getTotalSum = clContext.Compile(getTotalSum)

        let runInPlace2 = runInPlace2 opAdd zero clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<'a>) ->

            let numberOfGroups = inputArray.Length / valuesPerBlock + (if inputArray.Length % valuesPerBlock = 0 then 0 else 1)

            let carry =
                clContext.CreateClArrayWithSpecificAllocationMode<'a>(
                    DeviceOnly,
                    numberOfGroups
                )

            let totalSum = clContext.CreateClCell<'a>()

            let ndRangePreScan = Range1D.CreateValid(numberOfGroups * blockSize, blockSize)

            let preScanKernel = preScan.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(fun () -> preScanKernel.KernelFunc ndRangePreScan inputArray.Length inputArray carry totalSum)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(preScanKernel))

            if numberOfGroups > 1 then
                runInPlace2 processor carry

                let ndRangeScan = Range1D.CreateValid((numberOfGroups - 1) * valuesPerBlock, blockSize)

                let scan = scan.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments(fun () -> scan.KernelFunc ndRangeScan inputArray.Length inputArray carry totalSum)
                )

                processor.Post(Msg.CreateRunMsg<_, _>(scan))
            else
                let ndRangeTotalSum = Range1D.CreateValid(1, 1)
                let getTotalSum = getTotalSum.GetKernel()
                processor.Post(
                    Msg.MsgSetArguments(fun () -> getTotalSum.KernelFunc ndRangeTotalSum inputArray.Length inputArray totalSum)
                )
                processor.Post(Msg.CreateRunMsg<_, _>(getTotalSum))

            carry.Free processor

            totalSum

    let runExcludeInPlace plus zero = runInPlace1 plus zero

    /// <summary>
    /// Exclude in-place prefix sum of integer array with addition operation and start value that is equal to 0.
    /// </summary>
    /// <example>
    /// <code>
    /// let arr = [| 1; 1; 1; 1 |]
    /// let sum = [| 0 |]
    /// runExcludeInplace clContext workGroupSize processor arr sum (+) 0
    /// |> ignore
    /// ...
    /// > val arr = [| 0; 1; 2; 3 |]
    /// > val sum = [| 4 |]
    /// </code>
    /// </example>
    /// <param name="clContext">ClContext.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let standardExcludeInPlace (clContext: ClContext) workGroupSize =

        let scan =
            runExcludeInPlace <@ (+) @> 0 clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (inputArray: ClArray<int>) ->

            scan processor inputArray
