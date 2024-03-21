/**
 * Dremel-assembly of arrays of values into lists
 *
 * Reconstructs a complex nested structure from flat arrays of definition and repetition levels,
 * according to Dremel encoding. This simplified version focuses on arrays and scalar values,
 * with optional support for null values.
 *
 * @param {number[] | undefined} definitionLevels definition levels, max 3
 * @param {number[]} repetitionLevels repetition levels, max 1
 * @param {ArrayLike<any>} values values to process
 * @param {boolean} isNull can an entry be null?
 * @param {number} maxDefinitionLevel definition level that corresponds to non-null
 * @param {number} maxRepetitionLevel repetition level that corresponds to a new row
 * @returns {any[]} array of values
 */
export function assembleObjects(
  definitionLevels, repetitionLevels, values, isNull, maxDefinitionLevel, maxRepetitionLevel
) {
  let valueIndex = 0
  /** @type {any[]} */
  const output = []
  let currentContainer = output

  // Trackers for nested structures.
  const containerStack = [output]

  for (let i = 0; i < repetitionLevels.length; i++) {
    const def = definitionLevels?.length ? definitionLevels[i] : maxDefinitionLevel
    const rep = repetitionLevels[i]

    if (rep !== maxRepetitionLevel) {
      // Move back to the parent container
      while (rep < containerStack.length - 1) {
        containerStack.pop()
      }
      // Construct new lists up to max repetition level
      // @ts-expect-error won't be empty
      currentContainer = containerStack.at(-1)
      if (def) {
        for (let j = rep; j < maxRepetitionLevel; j++) {
          /** @type {any[]} */
          const newList = []
          currentContainer.push(newList)
          currentContainer = newList
          containerStack.push(newList)
        }
      }
    }

    // Add value or null based on definition level
    if (def === maxDefinitionLevel) {
      currentContainer.push(values[valueIndex++])
    } else if (isNull && def < maxDefinitionLevel) {
      // Go up one level to add null
      if (def) {
        containerStack.pop()
        // @ts-expect-error won't be empty
        currentContainer = containerStack.at(-1)
      } else {
        currentContainer.push(undefined)
      }
    }
  }

  // Handle edge cases for empty inputs or single-level data
  if (output.length === 0) {
    if (values.length > 0 && maxRepetitionLevel === 0) {
      // All values belong to the same (root) list
      return [values]
    }
    // return max definition level of nested lists
    /** @type {any[]} */
    for (let i = 0; i < maxDefinitionLevel; i++) {
      /** @type {any[]} */
      const newList = []
      currentContainer.push(newList)
      currentContainer = newList
    }
  }

  return output
}
