import { describe, expect, it } from 'vitest'

import {
  LIFECYCLES,
  PRESSURES,
  READINESS_REASONS,
  READINESS_STATUSES,
} from '../api/overview'
import {
  LIFECYCLE_LABELS,
  LIFECYCLE_TONES,
  overviewErrorLabel,
  PRESSURE_LABELS,
  PRESSURE_TONES,
  READINESS_REASON_LABELS,
  READINESS_STATUS_LABELS,
  READINESS_STATUS_TONES,
  readinessReasonLabel,
} from './labels'

const GENERIC_ERROR_LABEL = 'The station reported a temporary problem. Retrying automatically.'

describe('fixed enum label and tone maps', () => {
  it('covers every lifecycle exhaustively', () => {
    for (const lifecycle of LIFECYCLES) {
      expect(LIFECYCLE_LABELS[lifecycle], lifecycle).toBeTypeOf('string')
      expect(LIFECYCLE_LABELS[lifecycle].length, lifecycle).toBeGreaterThan(0)
      expect(LIFECYCLE_TONES[lifecycle], lifecycle).toBeTypeOf('string')
    }
    expect(Object.keys(LIFECYCLE_LABELS).sort()).toEqual([...LIFECYCLES].sort())
  })

  it('covers every pressure exhaustively', () => {
    for (const pressure of PRESSURES) {
      expect(PRESSURE_LABELS[pressure], pressure).toBeTypeOf('string')
      expect(PRESSURE_TONES[pressure], pressure).toBeTypeOf('string')
    }
    expect(Object.keys(PRESSURE_LABELS).sort()).toEqual([...PRESSURES].sort())
  })

  it('covers every readiness status exhaustively', () => {
    for (const status of READINESS_STATUSES) {
      expect(READINESS_STATUS_LABELS[status], status).toBeTypeOf('string')
      expect(READINESS_STATUS_TONES[status], status).toBeTypeOf('string')
    }
    expect(Object.keys(READINESS_STATUS_LABELS).sort()).toEqual([...READINESS_STATUSES].sort())
  })

  it('labels every non-NONE readiness reason and hides NONE', () => {
    for (const reason of READINESS_REASONS) {
      if (reason === 'NONE') {
        expect(readinessReasonLabel(reason)).toBeNull()
      } else {
        expect(readinessReasonLabel(reason)).toBe(READINESS_REASON_LABELS[reason])
      }
    }
    expect(Object.keys(READINESS_REASON_LABELS).sort()).toEqual(
      [...READINESS_REASONS].filter((reason) => reason !== 'NONE').sort(),
    )
  })
})

describe('overviewErrorLabel', () => {
  it('maps bounded reason classes to fixed copy', () => {
    expect(overviewErrorLabel('CAPACITY_EXCEEDED')).toBe(
      'The station is limiting new work. Retrying automatically.',
    )
    expect(overviewErrorLabel('RATE_LIMITED')).toBe(
      'The station is limiting new work. Retrying automatically.',
    )
    expect(overviewErrorLabel('STORAGE_UNAVAILABLE')).toBe(
      'The station is temporarily unavailable. Retrying automatically.',
    )
    expect(overviewErrorLabel('INTERNAL')).toBe(
      'The station is temporarily unavailable. Retrying automatically.',
    )
  })

  it('falls back to generic copy for unknown reasons', () => {
    expect(overviewErrorLabel('SOMETHING_NEW')).toBe(GENERIC_ERROR_LABEL)
    expect(overviewErrorLabel('')).toBe(GENERIC_ERROR_LABEL)
  })

  it('never echoes hostile raw reason text', () => {
    const hostile = '<img src=x onerror=alert(1)> /var/lib/station ns_abcdefghijklmnopqrstuv'
    const label = overviewErrorLabel(hostile)
    expect(label).toBe(GENERIC_ERROR_LABEL)
    expect(label).not.toContain(hostile)
    expect(label).not.toContain('<')
  })
})
