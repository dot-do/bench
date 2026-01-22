/**
 * IoT Timeseries Synthetic OLTP Dataset Generator
 *
 * Generates fake data for: devices, sensors, readings
 * Uses deterministic seeding for reproducibility.
 */

import * as fs from 'fs'
import * as path from 'path'

// Seeded random number generator (Mulberry32)
function createRng(seed: number) {
  return function () {
    let t = (seed += 0x6d2b79f5)
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

type SizeOption = '1mb' | '10mb' | '100mb' | '1gb'

interface GenerateOptions {
  size?: SizeOption
}

const SIZE_CONFIGS: Record<SizeOption, { devices: number; sensors: number; readings: number }> = {
  '1mb': { devices: 50, sensors: 150, readings: 50000 },
  '10mb': { devices: 500, sensors: 1500, readings: 500000 },
  '100mb': { devices: 5000, sensors: 15000, readings: 5000000 },
  '1gb': { devices: 50000, sensors: 150000, readings: 50000000 },
}

// Sample data
const DEVICE_TYPES = ['temperature_sensor', 'humidity_sensor', 'pressure_sensor', 'motion_detector', 'smart_meter', 'air_quality', 'vibration_sensor', 'flow_meter', 'level_sensor', 'gateway']
const DEVICE_CATEGORIES = ['sensor', 'actuator', 'gateway', 'controller', 'meter']
const MANUFACTURERS = ['SensorCorp', 'IoTech', 'SmartSense', 'DataFlow', 'EdgeDevices', 'ConnectAll', 'MetricPro', 'FlowMaster', 'TempTech', 'EnviroSense']
const LOCATION_TYPES = ['building', 'floor', 'room', 'zone', 'outdoor', 'vehicle']
const DEVICE_STATUSES = ['online', 'offline', 'maintenance', 'error', 'provisioning']
const METRIC_NAMES = ['temperature', 'humidity', 'pressure', 'co2', 'pm25', 'voltage', 'current', 'power', 'flow_rate', 'vibration']
const METRIC_UNITS: Record<string, string> = {
  temperature: 'celsius',
  humidity: 'percent',
  pressure: 'hPa',
  co2: 'ppm',
  pm25: 'ug/m3',
  voltage: 'volts',
  current: 'amps',
  power: 'watts',
  flow_rate: 'l/min',
  vibration: 'mm/s',
}
const METRIC_RANGES: Record<string, [number, number]> = {
  temperature: [-20, 50],
  humidity: [0, 100],
  pressure: [950, 1050],
  co2: [300, 2000],
  pm25: [0, 500],
  voltage: [100, 250],
  current: [0, 50],
  power: [0, 10000],
  flow_rate: [0, 100],
  vibration: [0, 50],
}
const QUALITY_LEVELS = ['good', 'uncertain', 'bad', 'unknown']
const BUILDING_NAMES = ['Building A', 'Building B', 'Main Office', 'Warehouse', 'Factory', 'Data Center', 'Lab', 'Campus']

function pick<T>(arr: T[], rng: () => number): T {
  return arr[Math.floor(rng() * arr.length)]
}

function generateUuid(rng: () => number): string {
  const hex = '0123456789abcdef'
  let uuid = ''
  for (let i = 0; i < 36; i++) {
    if (i === 8 || i === 13 || i === 18 || i === 23) {
      uuid += '-'
    } else if (i === 14) {
      uuid += '4'
    } else if (i === 19) {
      uuid += hex[(Math.floor(rng() * 4) + 8)]
    } else {
      uuid += hex[Math.floor(rng() * 16)]
    }
  }
  return uuid
}

function generateTimestamp(rng: () => number, startYear: number, endYear: number): string {
  const start = new Date(startYear, 0, 1).getTime()
  const end = new Date(endYear, 11, 31).getTime()
  const timestamp = new Date(start + rng() * (end - start))
  return timestamp.toISOString()
}

function generateSerialNumber(rng: () => number): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
  let serial = ''
  for (let i = 0; i < 12; i++) {
    serial += chars[Math.floor(rng() * chars.length)]
  }
  return serial
}

function generateMacAddress(rng: () => number): string {
  const hex = '0123456789ABCDEF'
  const parts: string[] = []
  for (let i = 0; i < 6; i++) {
    parts.push(hex[Math.floor(rng() * 16)] + hex[Math.floor(rng() * 16)])
  }
  return parts.join(':')
}

function generateIpAddress(rng: () => number): string {
  return `192.168.${Math.floor(rng() * 256)}.${Math.floor(rng() * 256)}`
}

function generateDevices(count: number, rng: () => number): any[] {
  const devices: any[] = []
  for (let i = 0; i < count; i++) {
    const deviceType = pick(DEVICE_TYPES, rng)
    const manufacturer = pick(MANUFACTURERS, rng)
    const status = pick(DEVICE_STATUSES, rng)

    devices.push({
      id: generateUuid(rng),
      serial_number: generateSerialNumber(rng),
      name: `${deviceType.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())} ${i + 1}`,
      device_type: deviceType,
      category: pick(DEVICE_CATEGORIES, rng),
      manufacturer,
      model: `${manufacturer.slice(0, 3).toUpperCase()}-${Math.floor(rng() * 9000) + 1000}`,
      firmware_version: `${Math.floor(rng() * 5) + 1}.${Math.floor(rng() * 10)}.${Math.floor(rng() * 100)}`,
      status,
      is_active: status !== 'maintenance' && rng() > 0.1,
      location: {
        name: `${pick(BUILDING_NAMES, rng)} - ${pick(LOCATION_TYPES, rng).replace(/\b\w/g, l => l.toUpperCase())} ${Math.floor(rng() * 100) + 1}`,
        type: pick(LOCATION_TYPES, rng),
        latitude: 37.7749 + (rng() - 0.5) * 0.1,
        longitude: -122.4194 + (rng() - 0.5) * 0.1,
        timezone: pick(['UTC', 'America/New_York', 'America/Los_Angeles', 'Europe/London', 'Asia/Tokyo'], rng),
      },
      network: {
        ip_address: generateIpAddress(rng),
        mac_address: generateMacAddress(rng),
        signal_strength: Math.floor(rng() * 100) - 100, // dBm
      },
      battery_level: rng() > 0.3 ? Math.floor(rng() * 100) : null, // Some devices are not battery powered
      report_interval_seconds: pick([1, 5, 15, 60, 300, 900], rng),
      tags: rng() > 0.5 ? ['production', 'monitored'] : ['testing'],
      config: {
        alerts_enabled: rng() > 0.3,
        low_battery_threshold: 20,
        offline_timeout_seconds: 300,
      },
      registered_at: generateTimestamp(rng, 2020, 2024),
      last_seen_at: status === 'online' ? generateTimestamp(rng, 2024, 2024) : generateTimestamp(rng, 2023, 2024),
      updated_at: generateTimestamp(rng, 2024, 2024),
    })
  }
  return devices
}

function generateSensors(count: number, deviceIds: string[], rng: () => number): any[] {
  const sensors: any[] = []
  for (let i = 0; i < count; i++) {
    const metricName = pick(METRIC_NAMES, rng)
    const [minVal, maxVal] = METRIC_RANGES[metricName]

    sensors.push({
      id: generateUuid(rng),
      device_id: pick(deviceIds, rng),
      name: `${metricName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())} Sensor`,
      metric_name: metricName,
      unit: METRIC_UNITS[metricName],
      min_value: minVal,
      max_value: maxVal,
      precision: metricName === 'temperature' || metricName === 'humidity' ? 2 : metricName === 'pressure' ? 1 : 0,
      calibration: {
        offset: (rng() - 0.5) * 2,
        scale: 0.98 + rng() * 0.04,
        last_calibrated_at: generateTimestamp(rng, 2023, 2024),
      },
      thresholds: {
        warning_low: minVal + (maxVal - minVal) * 0.1,
        warning_high: maxVal - (maxVal - minVal) * 0.1,
        critical_low: minVal + (maxVal - minVal) * 0.05,
        critical_high: maxVal - (maxVal - minVal) * 0.05,
      },
      is_active: rng() > 0.05,
      created_at: generateTimestamp(rng, 2020, 2024),
      updated_at: generateTimestamp(rng, 2024, 2024),
    })
  }
  return sensors
}

function generateReadings(count: number, sensors: any[], rng: () => number): any[] {
  const readings: any[] = []

  // Pre-compute sensor lookup for efficiency
  const sensorMap = new Map(sensors.map(s => [s.id, s]))

  // Generate readings with realistic time distribution
  // Readings are spread across the year with some clustering
  const baseTime = new Date(2024, 0, 1).getTime()
  const endTime = new Date(2024, 11, 31).getTime()
  const timeSpan = endTime - baseTime

  for (let i = 0; i < count; i++) {
    const sensor = pick(sensors, rng)
    const [minVal, maxVal] = METRIC_RANGES[sensor.metric_name] || [0, 100]

    // Generate value with some realistic variation (Gaussian-ish distribution around midpoint)
    const midpoint = (minVal + maxVal) / 2
    const range = maxVal - minVal
    // Box-Muller transform approximation
    const u1 = rng()
    const u2 = rng()
    const gaussian = Math.sqrt(-2 * Math.log(u1 + 0.0001)) * Math.cos(2 * Math.PI * u2)
    const value = midpoint + gaussian * range * 0.15 // Standard deviation of 15% of range

    // Clamp to valid range
    const clampedValue = Math.max(minVal, Math.min(maxVal, value))

    // Timestamp with some clustering (simulating regular reporting intervals)
    const timestamp = new Date(baseTime + rng() * timeSpan)

    readings.push({
      id: i + 1, // Using sequential IDs for readings (common in timeseries)
      sensor_id: sensor.id,
      device_id: sensor.device_id,
      metric_name: sensor.metric_name,
      timestamp: timestamp.toISOString(),
      value: Math.round(clampedValue * 1000) / 1000,
      unit: sensor.unit,
      quality: rng() < 0.92 ? 'good' : rng() < 0.97 ? 'uncertain' : rng() < 0.99 ? 'bad' : 'unknown',
      raw_value: Math.round((clampedValue / (sensor.calibration?.scale || 1) - (sensor.calibration?.offset || 0)) * 1000) / 1000,
    })
  }

  // Sort by timestamp for realistic timeseries ordering
  readings.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())

  return readings
}

function writeJsonl(filePath: string, data: any[]): void {
  const stream = fs.createWriteStream(filePath)
  for (const item of data) {
    stream.write(JSON.stringify(item) + '\n')
  }
  stream.end()
}

export async function generate(outputDir: string, options?: GenerateOptions): Promise<void> {
  const size = options?.size || '1mb'
  const config = SIZE_CONFIGS[size]
  const seed = 45678 // Fixed seed for reproducibility

  console.log(`Generating IoT dataset (${size})...`)

  // Create output directory if it doesn't exist
  fs.mkdirSync(outputDir, { recursive: true })

  // Generate with deterministic seed
  const rng = createRng(seed)

  console.log(`  Generating ${config.devices} devices...`)
  const devices = generateDevices(config.devices, rng)
  writeJsonl(path.join(outputDir, 'devices.jsonl'), devices)

  const deviceIds = devices.map(d => d.id)

  console.log(`  Generating ${config.sensors} sensors...`)
  const sensors = generateSensors(config.sensors, deviceIds, rng)
  writeJsonl(path.join(outputDir, 'sensors.jsonl'), sensors)

  console.log(`  Generating ${config.readings} readings...`)
  // For large datasets, generate in batches to avoid memory issues
  const batchSize = 100000
  const readingsPath = path.join(outputDir, 'readings.jsonl')

  if (config.readings <= batchSize) {
    const readings = generateReadings(config.readings, sensors, rng)
    writeJsonl(readingsPath, readings)
  } else {
    // Stream large datasets in batches
    const stream = fs.createWriteStream(readingsPath)
    let generated = 0
    let batchNum = 0

    while (generated < config.readings) {
      const batchCount = Math.min(batchSize, config.readings - generated)
      const readings = generateReadings(batchCount, sensors, rng)

      for (const reading of readings) {
        // Adjust IDs for batch offset
        reading.id = generated + readings.indexOf(reading) + 1
        stream.write(JSON.stringify(reading) + '\n')
      }

      generated += batchCount
      batchNum++
      if (batchNum % 10 === 0) {
        console.log(`    Generated ${generated.toLocaleString()} / ${config.readings.toLocaleString()} readings...`)
      }
    }

    stream.end()
  }

  console.log(`IoT dataset generated in ${outputDir}`)
}

// CLI support
if (typeof require !== 'undefined' && require.main === module) {
  const args = process.argv.slice(2)
  const outputDir = args[0] || './data/iot'
  const size = (args[1] as SizeOption) || '1mb'
  generate(outputDir, { size }).catch(console.error)
}
