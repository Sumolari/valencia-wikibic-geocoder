const parseCSV = require('csv-parse')
const stringifyToCSV = require('csv-stringify')
const fs = require('fs-extra')
const path = require('path')
const _ = require('lodash')
const NodeGeocoder = require('node-geocoder')
const async = require('async')
const log = require('captains-log')()

const conf = {}
require('rc')('app', conf)

const googleGeocoder = NodeGeocoder({
  provider: 'google',
  apiKey: _.get(conf, 'geocoding.google.api.key'),
  formatter: null
})

async function readCSV (pathToSource) {
  const data = await fs.readFile(
    path.resolve(__dirname, pathToSource)
  )

  return new Promise ((resolve, reject) => {
    parseCSV(data, {
      columns: true,
      trim: true,
      relax_column_count: true,
    }, (err, output) => {
      if (err) return reject(err)
      resolve(output)
    })
  })
}

async function geocode (data) {
  const dataWithIndex = _.map(data, (item, index) => {
    return _.assign({}, item, {rowIndex: index})
  })

  const itemsWithoutCoordinates = _.filter(
    dataWithIndex,
    item => !(item.lat && item.lon)
  )

  const itemsWithCoordinates = _.map(
    _.filter(dataWithIndex, item => item.lat && item.lon),
    item => {
      return _.assign({}, item, {
        latitude: item.lat,
        longitude: item.lon,
        manuallyGeocoded: true,
        automaticallyGeocoded: false,
        originalAddress: item.lugar,
        rowIndex: item.rowIndex,
      })
    }
  )

  const entries = _.map(itemsWithoutCoordinates, item => {
    return {
      address: item.lugar,
      city: 'Valencia',
      country: 'Spain',
    }
  })

  const rawResults = await googleGeocoder.batchGeocode(entries)

  const results = _.map(rawResults, (result, index) => {
    const source = itemsWithoutCoordinates[index].lugar
    const rowIndex = itemsWithoutCoordinates[index].rowIndex

    if (result.error) {
      return {
        rowIndex,
        source,
        manuallyGeocoded: false,
        automaticallyGeocoded: false,
        error: result.error,
      }
    }

    if (!result.value.length) {
      return {
        rowIndex,
        source,
        manuallyGeocoded: false,
        automaticallyGeocoded: false,
        error: 'No results found',
      }
    }

    const item =  _.assign(
      {},
      result.value[0],
      result.value[0].administrativeLevels,
      result.value[0].extra, {
        originalAddress: source,
        manuallyGeocoded: false,
        automaticallyGeocoded: true,
        rowIndex,
      }
    )

    delete item.administrativeLevels
    delete item.extra

    return item
  })

  const geocodedItems = [
    ...results,
    ...itemsWithCoordinates
  ]

  return _.map(geocodedItems, item => {
    if (item.error) return item
    return {
      rowIndex: item.rowIndex,
      latitude: item.latitude,
      longitude: item.longitude,
      originalAddress: item.originalAddress,
      manuallyGeocoded: item.manuallyGeocoded,
      automaticallyGeocoded: item.automaticallyGeocoded,
    }
  })
}

async function reverseGeocode (data) {
  const geocodingTasks = _.map(data, item => {
    return async () => {
      const results = await googleGeocoder.reverse({
        lat: item.latitude,
        lon: item.longitude,
      })
      return _.assign({}, results[0], {
        rowIndex: item.rowIndex,
      })
    }
  })

  return new Promise((resolve, reject) => {
    async.parallelLimit(
      geocodingTasks,
      10,
      (err, results) => {
        if (err) return reject(err)
        resolve(results)
      }
    )
  })
}

function toCache (data, file) {
  return fs.writeFile(
    path.resolve(__dirname, file),
    JSON.stringify(data)
  )
}

async function fromCache (file) {
  const jsonString = await fs.readFile(
    path.resolve(__dirname, file)
  )
  return JSON.parse(jsonString)
}

async function main () {
  const dataCache = 'data.cache.json'
  const geocodedCache = 'geocoded.cache.json'
  const reversedCache = 'reversed.cache.json'

  let data
  try {
    data = await fromCache(dataCache)
    log.debug(
      `Data loaded from cache (${data.length} rows)`
    )
  } catch (err) {
    data = await readCSV('./source.csv')
    await toCache(data, dataCache)
    log.info('Data written to cache')
  }

  let geocodedData
  try {
    geocodedData = await fromCache(geocodedCache)
    log.debug(
      `Geocoded data loaded from cache (${geocodedData.length} rows)`
    )
  } catch (err) {
    geocodedData = await geocode(data)
    await toCache(geocodedData, geocodedCache)
    log.info(`Geocoded data written to cache ${geocodedData.length} rows)`)
  }

  let reversedData
  try {
    reversedData = await fromCache(reversedCache)
    log.debug(
      `Reverse geocoded data loaded from cache (${reversedData.length} rows)`
    )
  } catch (err) {
    const successfullyGeocodedData = _.reject(
      geocodedData,
      'error'
    )
    reversedData = await reverseGeocode(
      successfullyGeocodedData
    )
    await toCache(reversedData, reversedCache)
    log.info(
      `Reverse geocoded data written to cache (${reversedData.length} rows)`
    )
  }

  const sourceDataByRowIndex = _.fromPairs(
    _.map(data, (item, rowIndex) => [rowIndex, item])
  )
  const geocodedDataByRowIndex = _.fromPairs(
    _.map(geocodedData, item => [item.rowIndex, item])
  )
  const reversedDataByRowIndex = _.fromPairs(
    _.map(reversedData, item => [item.rowIndex, item])
  )
  const itemsWithZipcodes = _.map(
    sourceDataByRowIndex, (item, rowIndex) => {
    return _.assign({}, item, {
      manuallyGeocoded: geocodedDataByRowIndex[rowIndex].manuallyGeocoded,
      automaticallyGeocoded: geocodedDataByRowIndex[rowIndex].automaticallyGeocoded,
      geocoded: geocodedDataByRowIndex[rowIndex].manuallyGeocoded || geocodedDataByRowIndex[rowIndex].automaticallyGeocoded,
      zipcode: _.get(
        reversedDataByRowIndex[rowIndex], 'zipcode'
      )
    })
  })

  const csvText = await new Promise ((resolve, reject) => {
    stringifyToCSV(itemsWithZipcodes, {
      header: true,
      formatters: {
        bool (value) {
          return value ? 'YES' : 'NO'
        }
      }
    }, function (err, output) {
      if (err) return reject(err)
      resolve(output)
    })
  })

  fs.writeFile(
    path.resolve(__dirname, './output.csv'),
    csvText
  )
}

main()
