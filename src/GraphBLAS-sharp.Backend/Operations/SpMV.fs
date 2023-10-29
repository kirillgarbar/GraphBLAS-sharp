namespace GraphBLAS.FSharp.Backend.Operations

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClContextExtensions

module internal SpMV =
    let runSPLATo
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let spmv =
            <@ fun (ndRange: Range2D)
                (numberOfRows: int)
                (numberOfGroups: int)
                (matrixPtr: ClArray<int>)
                (matrixColumns: ClArray<int>)
                (matrixValues: ClArray<'a>)
                (vectorValues: ClArray<'b option>)
                (resultValues: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0 // id of row
                let lid = ndRange.LocalID1  // id inside row
                let localSize = workGroupSize
                let globalStride = numberOfGroups

                let segmentSum = localArray<'c option> workGroupSize

                let mutable row = gid

                //Reduce one row on each iteration
                while row < numberOfRows do
                    if lid = 0 then resultValues.[row] <- None

                    let workStart = matrixPtr.[row]
                    let workEnd = matrixPtr.[row + 1]

                    let mutable sum: 'c option = None

                    let mutable i = workStart + lid

                    //Each thread reduces 1 / workGroupSize values to the local array
                    while i < workEnd do
                        let columnIndex = matrixColumns.[i]
                        let mulRes = (%mul) (Some matrixValues.[i]) vectorValues.[columnIndex] // Brahma exception
                        let res = (%add) sum mulRes
                        sum <- res
                        i <- i + localSize

                    segmentSum.[lid] <- sum

                    barrierLocal ()

                    let mutable blockSize = workGroupSize

                    //Reduce local array
                    while blockSize >= 2 do
                        let halfBlockSize = blockSize / 2
                        if lid < halfBlockSize then
                            let res = (%add) segmentSum.[lid] segmentSum.[lid + halfBlockSize]
                            segmentSum.[lid] <- res
                        blockSize <- halfBlockSize

                    if lid = 0 then
                        resultValues.[row] <- segmentSum.[0]

                    row <- row + globalStride
                    @>

        let spmv = clContext.Compile spmv

        fun (queue: MailboxProcessor<_>) (matrix: ClMatrix.CSR<'a>) (vector: ClArray<'b option>) (result: ClArray<'c option>) ->

            let nGroups = min matrix.RowCount 512

            let ndRange = Range2D (nGroups, workGroupSize, 1, workGroupSize)

            let kernel = spmv.GetKernel()

            queue.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            matrix.RowCount
                            nGroups
                            matrix.RowPointers
                            matrix.Columns
                            matrix.Values
                            vector
                            result)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(kernel))

    let runSPLA
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =
        let runTo = runSPLATo add mul clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) (vector: ClArray<'b option>) ->

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode<'c option>(allocationMode, matrix.RowCount)

            runTo queue matrix vector result

            result

    let runSimpleTo
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let spmv =
            <@ fun (ndRange: Range1D)
                (numberOfRows: int)
                (matrixPtr: ClArray<int>)
                (matrixColumns: ClArray<int>)
                (matrixValues: ClArray<'a>)
                (vectorValues: ClArray<'b option>)
                (resultValues: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0 // id of row

                if gid < numberOfRows then
                    let rowStart = matrixPtr.[gid]
                    let rowEnd = matrixPtr.[gid + 1]

                    let mutable sum: 'c option = None

                    for i in rowStart .. rowEnd - 1 do
                        let columnIndex = matrixColumns.[i]
                        let mulRes = (%mul) (Some matrixValues.[i]) vectorValues.[columnIndex] // Brahma exception
                        let res = (%add) sum mulRes
                        sum <- res

                    resultValues.[gid] <- sum @>

        let spmv = clContext.Compile spmv

        fun (queue: MailboxProcessor<_>) (matrix: ClMatrix.CSR<'a>) (vector: ClArray<'b option>) (result: ClArray<'c option>) ->

            let ndRange = Range1D.CreateValid(matrix.RowCount, workGroupSize)

            let kernel = spmv.GetKernel()

            queue.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        kernel.KernelFunc
                            ndRange
                            matrix.RowCount
                            matrix.RowPointers
                            matrix.Columns
                            matrix.Values
                            vector
                            result)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(kernel))

    let runSimple
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =
        let runTo = runSimpleTo add mul clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) (vector: ClArray<'b option>) ->

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode<'c option>(allocationMode, matrix.RowCount)

            runTo queue matrix vector result

            result

    let runTo
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let localMemorySize =
            clContext.ClDevice.LocalMemSize / 1<Byte>

        let localPointersArraySize = workGroupSize + 1

        let localMemoryLeft =
            localMemorySize
            - localPointersArraySize * sizeof<int>

        let localValuesArraySize =
            Utils.getClArrayOfOptionTypeSize localMemoryLeft

        let multiplyValues =
            <@ fun (ndRange: Range1D) matrixLength (matrixColumns: ClArray<int>) (matrixValues: ClArray<'a>) (vectorValues: ClArray<'b option>) (intermediateArray: ClArray<'c option>) ->

                let i = ndRange.GlobalID0
                let value = matrixValues.[i]
                let column = matrixColumns.[i]

                if i < matrixLength then
                    intermediateArray.[i] <- (%mul) (Some value) vectorValues.[column] @>

        let reduceValuesByRows =
            <@ fun (ndRange: Range1D) (numberOfRows: int) (intermediateArray: ClArray<'c option>) (matrixPtr: ClArray<int>) (outputVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localPtr = localArray<int> localPointersArraySize

                let localValues =
                    localArray<'c option> localValuesArraySize

                if gid <= numberOfRows then
                    let threadsPerBlock =
                        min (numberOfRows - gid + lid) workGroupSize //If number of rows left is lesser than number of threads in a block

                    localPtr.[lid] <- matrixPtr.[gid]

                    if lid = 0 then
                        localPtr.[threadsPerBlock] <- matrixPtr.[gid + threadsPerBlock]

                    barrierLocal ()

                    let workEnd = localPtr.[threadsPerBlock]
                    let mutable blockLowerBound = localPtr.[0]
                    let numberOfBlocksFitting = localValuesArraySize / threadsPerBlock
                    let workPerIteration = threadsPerBlock * numberOfBlocksFitting

                    let mutable sum: 'c option = None

                    while blockLowerBound < workEnd do
                        let mutable index = blockLowerBound + lid

                        barrierLocal ()
                        //Loading values to the local memory
                        for block in 0 .. numberOfBlocksFitting - 1 do
                            if index < workEnd then
                                localValues.[lid + block * threadsPerBlock] <- intermediateArray.[index]
                                index <- index + threadsPerBlock

                        barrierLocal ()
                        //Reduction
                        //Check if any part of the row is loaded into local memory on this iteration
                        if (localPtr.[lid + 1] > blockLowerBound
                            && localPtr.[lid] < blockLowerBound + workPerIteration) then
                            let rowStart = max (localPtr.[lid] - blockLowerBound) 0

                            let rowEnd =
                                min (localPtr.[lid + 1] - blockLowerBound) workPerIteration

                            for j in rowStart .. rowEnd - 1 do
                                let newSum = (%add) sum localValues.[j] //For some reason sum <- (%add) ... causes Brahma exception
                                sum <- newSum

                        blockLowerBound <- blockLowerBound + workPerIteration

                    if gid < numberOfRows then
                        outputVector.[gid] <- sum @>

        let multiplyValues = clContext.Compile multiplyValues
        let reduceValuesByRows = clContext.Compile reduceValuesByRows

        fun (queue: MailboxProcessor<_>) (matrix: ClMatrix.CSR<'a>) (vector: ClArray<'b option>) (result: ClArray<'c option>) ->

            let matrixLength = matrix.Values.Length

            let ndRange1 =
                Range1D.CreateValid(matrixLength, workGroupSize)

            let ndRange2 =
                Range1D.CreateValid(matrix.RowCount, workGroupSize)

            let intermediateArray =
                clContext.CreateClArrayWithSpecificAllocationMode<'c option>(DeviceOnly, matrixLength)

            let multiplyValues = multiplyValues.GetKernel()

            queue.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        multiplyValues.KernelFunc
                            ndRange1
                            matrixLength
                            matrix.Columns
                            matrix.Values
                            vector
                            intermediateArray)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(multiplyValues))

            let reduceValuesByRows = reduceValuesByRows.GetKernel()

            queue.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        reduceValuesByRows.KernelFunc
                            ndRange2
                            matrix.RowCount
                            intermediateArray
                            matrix.RowPointers
                            result)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(reduceValuesByRows))

            queue.Post(Msg.CreateFreeMsg intermediateArray)

    let run
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =
        let runTo = runTo add mul clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) (vector: ClArray<'b option>) ->

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode<'c option>(allocationMode, matrix.RowCount)

            runTo queue matrix vector result

            result
