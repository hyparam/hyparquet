import { assembleNested } from './assemble.js'
import { readColumn } from './column.js'
import { DEFAULT_PARSERS } from './convert.js'
import { getSchemaPath } from './schema.js'
import { flatten } from './utils.js'

/**
 * @import {AsyncColumn, AsyncRowGroup, DecodedArray, GroupPlan, ParquetParsers, ParquetReadOptions, QueryPlan, RowGroup, SchemaTree} from './types.js'
 */
/**
 * Read a row group from a file-like object.
 *
 * @param {ParquetReadOptions} options
 * @param {QueryPlan} plan
 * @param {GroupPlan} groupPlan
 * @returns {AsyncRowGroup} resolves to column data
 */
export function readRowGroup(options, { metadata }, groupPlan) {
  const { file, compressors, utf8 } = options

  /** @type {AsyncColumn[]} */
  const asyncColumns = []
  /** @type {ParquetParsers} */
  const parsers = { ...DEFAULT_PARSERS, ...options.parsers }

  // read column data
  for (const { columnMetadata, range } of groupPlan.chunks) {
    const columnBytes = range.endByte - range.startByte

    // skip columns larger than 1gb
    // TODO: stream process the data, returning only the requested rows
    if (columnBytes > 1 << 30) {
      console.warn(`parquet skipping huge column "${columnMetadata.path_in_schema}" ${columnBytes} bytes`)
      // TODO: set column to new Error('parquet column too large')
      continue
    }

    // wrap awaitable to ensure it's a promise
    /** @type {Promise<ArrayBuffer>} */
    const buffer = Promise.resolve(file.slice(range.startByte, range.endByte))

    // read column data async
    asyncColumns.push({
      pathInSchema: columnMetadata.path_in_schema,
      data: buffer.then(arrayBuffer => {
        const schemaPath = getSchemaPath(metadata.schema, columnMetadata.path_in_schema)
        const reader = { view: new DataView(arrayBuffer), offset: 0 }
        const columnDecoder = {
          pathInSchema: columnMetadata.path_in_schema,
          type: columnMetadata.type,
          element: schemaPath[schemaPath.length - 1].element,
          schemaPath,
          codec: columnMetadata.codec,
          parsers,
          compressors,
          utf8,
        }
        return readColumn(reader, groupPlan, columnDecoder, options.onPage)
      }),
    })
  }

  return { groupStart: groupPlan.groupStart, groupRows: groupPlan.groupRows, asyncColumns }
}

/**
 * @overload
 * @param {AsyncRowGroup} asyncGroup
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @param {'object'} rowFormat
 * @returns {Promise<Record<string, any>[]>} resolves to row data
 */
/**
 * @overload
 * @param {AsyncRowGroup} asyncGroup
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @param {'array'} [rowFormat]
 * @returns {Promise<any[][]>} resolves to row data
 */
/**
 * @param {AsyncRowGroup} asyncGroup
 * @param {number} selectStart
 * @param {number} selectEnd
 * @param {string[] | undefined} columns
 * @param {'object' | 'array'} [rowFormat]
 * @returns {Promise<Record<string, any>[] | any[][]>} resolves to row data
 */
export async function asyncGroupToRows({ asyncColumns }, selectStart, selectEnd, columns, rowFormat) {
  // columnData[i] for asyncColumns[i]
  // TODO: do it without flatten
  const columnDatas = await Promise.all(asyncColumns.map(({ data }) => data.then(flatten)))

  // careful mapping of column order for rowFormat: array
  const includedColumnNames = asyncColumns
    .map(child => child.pathInSchema[0])
    .filter(name => !columns || columns.includes(name))
  const columnOrder = columns ?? includedColumnNames
  const columnIndexes = columnOrder.map(name => asyncColumns.findIndex(column => column.pathInSchema[0] === name))

  // transpose columns into rows
  const selectCount = selectEnd - selectStart
  if (rowFormat === 'object') {
    /** @type {Record<string, any>[]} */
    const groupData = Array(selectCount)
    for (let selectRow = 0; selectRow < selectCount; selectRow++) {
      const row = selectStart + selectRow
      // return each row as an object
      /** @type {Record<string, any>} */
      const rowData = {}
      for (let i = 0; i < asyncColumns.length; i++) {
        rowData[asyncColumns[i].pathInSchema[0]] = columnDatas[i][row]
      }
      groupData[selectRow] = rowData
    }
    return groupData
  }

  /** @type {any[][]} */
  const groupData = Array(selectCount)
  for (let selectRow = 0; selectRow < selectCount; selectRow++) {
    const row = selectStart + selectRow
    // return each row as an array
    const rowData = Array(asyncColumns.length)
    for (let i = 0; i < columnOrder.length; i++) {
      if (columnIndexes[i] >= 0) {
        rowData[i] = columnDatas[columnIndexes[i]][row]
      }
    }
    groupData[selectRow] = rowData
  }
  return groupData
}

/**
 * Assemble physical columns into top-level columns asynchronously.
 *
 * @param {AsyncRowGroup} asyncRowGroup
 * @param {SchemaTree} schemaTree
 * @returns {AsyncRowGroup}
 */
export function assembleAsync(asyncRowGroup, schemaTree) {
  const { asyncColumns } = asyncRowGroup
  /** @type {AsyncColumn[]} */
  const assembled = []
  for (const child of schemaTree.children) {
    if (child.children.length) {
      const childColumns = asyncColumns.filter(column => column.pathInSchema[0] === child.element.name)
      if (!childColumns.length) continue

      // wait for all child columns to be read
      /** @type {Map<string, DecodedArray>} */
      const flatData = new Map()
      const data = Promise.all(childColumns.map(column => {
        return column.data.then(columnData => {
          flatData.set(column.pathInSchema.join('.'), flatten(columnData))
        })
      })).then(() => {
        // assemble the column
        assembleNested(flatData, child)
        const flatColumn = flatData.get(child.path.join('.'))
        if (!flatColumn) throw new Error('parquet column data not assembled')
        return [flatColumn]
      })

      assembled.push({ pathInSchema: child.path, data })
    } else {
      // leaf node, return the column
      const asyncColumn = asyncColumns.find(column => column.pathInSchema[0] === child.element.name)
      if (asyncColumn) {
        assembled.push(asyncColumn)
      }
    }
  }
  return { ...asyncRowGroup, asyncColumns: assembled }
}
