import { describe, expect, it } from 'vitest'

import { formatBytes, formatCount, formatPercent, formatRelativeAge } from './format'

describe('formatCount', () => {
  it('formats integers with pinned grouping', () => {
    expect(formatCount(0)).toBe('0')
    expect(formatCount(128)).toBe('128')
    expect(formatCount(4096)).toBe('4,096')
    expect(formatCount(100000)).toBe('100,000')
  })

  it('clamps non-finite and negative input', () => {
    expect(formatCount(Number.NaN)).toBe('0')
    expect(formatCount(-5)).toBe('0')
  })
})

describe('formatBytes', () => {
  it('formats IEC units deterministically', () => {
    expect(formatBytes(0)).toBe('0 B')
    expect(formatBytes(512)).toBe('512 B')
    expect(formatBytes(1024)).toBe('1 KiB')
    expect(formatBytes(268435456)).toBe('256 MiB')
    expect(formatBytes(1610612736)).toBe('1.5 GiB')
    expect(formatBytes(4294967296)).toBe('4 GiB')
  })

  it('clamps non-finite and negative input', () => {
    expect(formatBytes(Number.NaN)).toBe('0 B')
    expect(formatBytes(-1)).toBe('0 B')
  })
})

describe('formatPercent', () => {
  it('formats clamped integer percents', () => {
    expect(formatPercent(0)).toBe('0%')
    expect(formatPercent(0.004)).toBe('0%')
    expect(formatPercent(0.126)).toBe('13%')
    expect(formatPercent(1)).toBe('100%')
  })

  it('clamps out-of-range and non-finite input', () => {
    expect(formatPercent(1.5)).toBe('100%')
    expect(formatPercent(-0.5)).toBe('0%')
    expect(formatPercent(Number.NaN)).toBe('0%')
  })
})

describe('formatRelativeAge', () => {
  it('buckets ages coarsely', () => {
    expect(formatRelativeAge(0)).toBe('just now')
    expect(formatRelativeAge(4999)).toBe('just now')
    expect(formatRelativeAge(5000)).toBe('5 s ago')
    expect(formatRelativeAge(45000)).toBe('45 s ago')
    expect(formatRelativeAge(60000)).toBe('1 min ago')
    expect(formatRelativeAge(3599000)).toBe('59 min ago')
    expect(formatRelativeAge(3600000)).toBe('1 h ago')
    expect(formatRelativeAge(90000000)).toBe('25 h ago')
  })

  it('treats invalid ages as just now', () => {
    expect(formatRelativeAge(-100)).toBe('just now')
    expect(formatRelativeAge(Number.NaN)).toBe('just now')
  })
})
