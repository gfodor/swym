(function() {
  var ConsoleReporter, jasmineEnv,
    __hasProp = Object.prototype.hasOwnProperty,
    __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor; child.__super__ = parent.prototype; return child; };

  jasmineEnv = jasmine.getEnv();

  jasmineEnv.updateInterval = 1000;

  ConsoleReporter = (function(_super) {

    __extends(ConsoleReporter, _super);

    function ConsoleReporter() {
      ConsoleReporter.__super__.constructor.apply(this, arguments);
    }

    ConsoleReporter.prototype.reportRunnerResults = function(runner) {
      var message, results, specCount, specs;
      results = runner.results();
      specs = runner.specs();
      specCount = specs.length;
      message = ">> " + specCount + " spec" + (specCount === 1 ? "" : "s") + ", " + results.failedCount + " failure" + (results.failedCount === 1 ? "" : "s");
      return console.log(message);
    };

    ConsoleReporter.prototype.reportSuiteResults = function(suite) {
      var results, status;
      results = suite.results();
      status = results.passed() ? 'passed' : 'failed';
      return console.log(">> Suite: " + status);
    };

    ConsoleReporter.prototype.reportSpecStarting = function(spec) {
      return console.log('>> Jasmine Running ' + spec.suite.description + ' ' + spec.description + '...');
    };

    ConsoleReporter.prototype.reportSpecResults = function(spec) {
      var message, result, resultItems, results, status, _i, _len, _ref, _results;
      results = spec.results();
      status = (_ref = results.passed()) != null ? _ref : {
        'passed': 'failed'
      };
      if (results.skipped) status = 'skipped';
      message = spec.description;
      resultItems = results.getItems();
      _results = [];
      for (_i = 0, _len = resultItems.length; _i < _len; _i++) {
        result = resultItems[_i];
        if (result.type === 'log') {
          _results.push(console.log(result));
        } else if (result.type === 'expect' && result.passed && !result.passed()) {
          console.log(result.message);
          _results.push(console.log(result.trace.stack));
        } else {
          _results.push(void 0);
        }
      }
      return _results;
    };

    ConsoleReporter.prototype.log = console.log;

    return ConsoleReporter;

  })(jasmine.Reporter);

  jasmineEnv.addReporter(new ConsoleReporter());

  jasmineEnv.execute();

}).call(this);
