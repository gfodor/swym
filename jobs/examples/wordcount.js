(function() {

  job(function($, _) {
    return $.flow('word count', function() {
      var assembly;
      $.source('input', $.tap("data/listings.txt", $.text_line_scheme("offset", "line")));
      assembly = $.assembly('input', function() {
        $.map({
          add: ["word"],
          remove: ["line", "offset"]
        }, function(tuple, writer) {
          var word, _i, _len, _ref, _results;
          _ref = tuple.line.match(/\S+/g);
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            word = _ref[_i];
            _results.push(writer({
              word: word
            }));
          }
          return _results;
        });
        $.map({}, function(tuple, writer) {
          tuple.word = tuple.word.toUpperCase();
          return writer(tuple);
        });
        return $.map({
          add: ["foo"]
        }, function(tuple, writer) {
          tuple.foo = "hi";
          return writer(tuple);
        });
      });
      return $.sink('input', $.tap("output", $.text_line_scheme("word", "foo")));
    });
  });

}).call(this);
