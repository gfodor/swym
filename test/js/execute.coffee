jasmineEnv = jasmine.getEnv()
jasmineEnv.updateInterval = 1000

class ConsoleReporter extends jasmine.Reporter
  reportRunnerResults: (runner) ->
    results = runner.results()
    specs = runner.specs()
    specCount = specs.length

    message = ">> #{specCount} spec#{(if specCount == 1 then "" else "s")}, #{results.failedCount} failure#{(if results.failedCount == 1 then "" else "s")}"
    console.log(message)

  reportSuiteResults: (suite) ->
    results = suite.results()
    status = if results.passed() then 'passed' else 'failed'
    console.log(">> Suite: #{status}")

  reportSpecStarting: (spec) ->
    console.log('>> Jasmine Running ' + spec.suite.description + ' ' + spec.description + '...')

  reportSpecResults: (spec) ->
    results = spec.results()
    status = results.passed() ? 'passed' : 'failed'
    status = 'skipped' if results.skipped

    message = spec.description
    resultItems = results.getItems()

    for result in resultItems
      if result.type == 'log'
        console.log(result)
      else if result.type == 'expect' && result.passed && !result.passed()
        console.log(result.message)
        console.log(result.trace.stack)

  log: console.log

jasmineEnv.addReporter(new ConsoleReporter())
jasmineEnv.execute()
