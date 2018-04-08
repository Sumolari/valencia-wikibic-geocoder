const fs = require('fs-extra')
const path = require('path')
const _ = require('lodash')

async function convertToCSV (
  pathToSource,
  pathToOutput
) {
  const data = await fs.readFile(
    path.resolve(__dirname, pathToSource)
  )

  const lines = _.map(
    data.toString().split('\n'),
    line => line.trim()
  )

  const rowsData = []
  let rowStarted
  let currentRow

  for (const line of lines) {
    if (line === '{{fila BIC') {
      rowStarted = true
      currentRow = []
    }
    else if (line === '}}') {
      rowsData.push(currentRow)
      rowStarted = false
    }
    else if (rowStarted) {
      currentRow.push(line)
    }
  }

  let rowHeaders
  const rowValues = _.map(rowsData, rd => {
    rowHeaders = []
    const rowValues = _.flatten(_.map(rd, columnData => {
      if (
        /^\s*\|\s*lat\s*=\s*[^|]*\|\s*lon\s*=.*$/.test(
          columnData
        )
      ) {
        const match = /^\s*\|\s*lat\s*=\s*([^|]*)\|\s*lon\s*=(.*)$/gi.exec(columnData)
        const lat = match[1].trim()
        const lon = match[2].trim()
        rowHeaders.push('lat')
        rowHeaders.push('lon')
        return [`"${lat}"`, `"${lon}"`]
      }

      const match = /\|([^=]*)=/gi.exec(columnData)
      rowHeaders.push(match[1])
      const value = columnData
        .replace(/[^=]*=/gi, '')
        .replace(/"/gi, '')
        .trim()
      return [`"${value}"`]
    }))

    if (rowValues[0].includes('Acequia')) {
      return null
    }

    return rowValues.join(',')
  })

  const validRows = _.filter(rowValues)

  const headers = _.map(rowHeaders, value =>  {
    return `"${value.replace(/"/gi, '').trim()}"`
  }).join(',')

  await fs.writeFile(
    path.resolve(__dirname, pathToOutput),
    [headers, ...validRows].join('\n')
  )
}

async function main () {
  await convertToCSV('./source.txt', './source.csv')
}

main()