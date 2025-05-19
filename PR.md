

`AsyncRowGroup` is made up of `AsyncColumn`s. Collectively this gives a map of the incoming data. This abstraction is designed to be a direct representation of how **parquet** data is fetched. The goal is to make it easy to build on top of this abstraction, and not get in the way of any ounce of performance. Data is returned as lists-of-lists to avoid concating unless necessary downstream.

Want to built `onComplete` from `AsyncRowGroup`? It's in the PR.

Want to build `onChunk` from `AsyncRowGroup`? It's in the PR.

Want to build `getColumn`? The current implementation in cli requires complex caching and re-assembly and sorting. No more re-assembly from `onChunk` with `AsyncRowGroup`:

```js
function getColumn(file, columnName) {
  const plan = parquetPlan({ metadata, columns: [columnName] })
  const asyncRowGroups = readRowGroups({ file }, plan)
  const columnData = asyncRowGroups.map(rg => rg.asyncColumns[0].data).flatten().flatten()
  return columnData
}
```

Want to build an `AsyncGenerator` that yields row groups as row objects?

```js
async function* readRowGroupsAsync({ file, columns }) {
  const plan = parquetPlan({ metadata, columns })
  const asyncRowGroups = readRowGroups({ file }, plan)
  for (const rg of asyncRowGroups) {
    yield await asyncGroupToRows(rg, 0, rg.groupRows, columns)
  }
}
```

Want materialized column data for maximally efficient writing with hyparquet-writer?

```js
async function* readColumnData({ file, columns }) {
  const plan = parquetPlan({ metadata, columns })
  const asyncRowGroups = readRowGroups({ file }, plan)
  for (const rg of asyncRowGroups) {
    yield rg.asyncColumns.map(asyncColumn => {
      return {
        columnName: asyncColumn.columnName,
        data: await asyncColumn.data,
      }
    })
  }
}
```
