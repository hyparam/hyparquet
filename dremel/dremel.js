import { assembleObjects } from '../src/assemble.js'

const defs = document.getElementById('defs')
const reps = document.getElementById('reps')
const values = document.getElementById('values')
const valuesWithNulls = document.getElementById('values-with-nulls')
const output = document.getElementById('output')

// update the output on change
defs.addEventListener('keyup', update)
reps.addEventListener('keyup', update)
values.addEventListener('keyup', update)

function update() {
  let def = defs.value ? defs.value.split(',').map(Number) : []
  const rep = reps.value.split(',').map(Number)
  const val = values.value.split(',').map(Number)
  const maxDef = Math.max(1, ...def)
  const maxRep = Math.max(...rep)
  // nullable if any definition level is less than max
  const isNullable = def.some(d => d < maxDef)
  if (def.length === 0) def = undefined

  // update flattened values with nulls
  const withNulls = []
  let valueIndex = 0
  for (let i = 0; i < rep.length; i++) {
    if (!isNullable || def[i] === maxDef) withNulls.push(val[valueIndex++])
    else withNulls.push('-')
  }
  valuesWithNulls.innerText = withNulls.join(', ')

  // update the output
  try {
    const out = assembleObjects(def, rep, val, isNullable, maxDef, maxRep)
    output.innerText = ''
    for (const obj of out) {
      const row = JSON.stringify(obj, null, 1)?.replace(/\s+/g, ' ')
      output.innerText += (row || 'null') + '\n'
    }
    output.classList.remove('error')
  } catch (e) {
    output.innerHTML = e.toString()
    output.classList.add('error')
  }
}
update()
