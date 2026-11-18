import { describe, expect, it } from 'vitest'

import { createOperatorSession } from './operatorSession'

describe('operatorSession', () => {
  it('starts empty', () => {
    const session = createOperatorSession()
    expect(session.hasCredential()).toBe(false)
    expect(session.getCredential()).toBeNull()
  })

  it('stores and returns one credential in memory', () => {
    const session = createOperatorSession()
    session.setCredential('test-credential')
    expect(session.hasCredential()).toBe(true)
    expect(session.getCredential()).toBe('test-credential')
  })

  it('rejects an empty credential', () => {
    const session = createOperatorSession()
    session.setCredential('')
    expect(session.hasCredential()).toBe(false)
    expect(session.getCredential()).toBeNull()
  })

  it('replaces the previous credential on set', () => {
    const session = createOperatorSession()
    session.setCredential('first-credential')
    session.setCredential('second-credential')
    expect(session.getCredential()).toBe('second-credential')
  })

  it('wipes the credential on clear', () => {
    const session = createOperatorSession()
    session.setCredential('test-credential')
    session.clear()
    expect(session.hasCredential()).toBe(false)
    expect(session.getCredential()).toBeNull()
  })
})
