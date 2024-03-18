/**
 * Dremel-assembly of arrays of values into lists
 *
 * @param {number[] | undefined} definitionLevels definition levels, max 3
 * @param {number[]} repetitionLevels repetition levels, max 1
 * @param {ArrayLike<any>} value values to process
 * @param {boolean} isNull can an entry be null?
 * @param {number} maxDefinitionLevel definition level that corresponds to non-null
 * @returns {any[]} array of values
 */
export function assembleObjects(
  definitionLevels, repetitionLevels, value, isNull, maxDefinitionLevel
) {
  let vali = 0
  let started = false
  let haveNull = false
  let i = 0
  let part = []
  /** @type {any[]} */
  const assign = []

  for (let counter = 0; counter < repetitionLevels.length; counter++) {
    const def = definitionLevels?.length ? definitionLevels[counter] : maxDefinitionLevel
    const rep = repetitionLevels[counter]

    if (!rep) {
      // new row - save what we have
      if (started) {
        assign[i] = haveNull ? undefined : part
        part = []
        i++
      } else {
        // first time: no row to save yet, unless it's a row continued from previous page
        if (vali > 0) {
          assign[i - 1] = assign[i - 1]?.concat(part) // add items to previous row
          part = []
          // don't increment i since we only filled i-1
        }
        started = true
      }
    }

    if (def === maxDefinitionLevel) {
      // append real value to current item
      part.push(value[vali])
      vali++
    } else if (def > 0) {
      // append null to current item
      part.push(undefined)
    }

    haveNull = def === 0 && isNull
  }

  if (started) {
    assign[i] = haveNull ? undefined : part
  }

  return assign
}
