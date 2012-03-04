(function() {

  job(function($, _) {
    return $.flow('word count', function() {
      var assembly;
      $.source('input', $.tap("data/listings.txt", $.text_line_scheme("offset", "line")));
      assembly = $.assembly('input', function() {
        var count, last_key;
        $.map({
          add: ["word", "word2"],
          remove: ["line", "offset"]
        }, function(tuple, writer) {
          var word, _i, _len, _ref, _results;
          _ref = tuple.line.match(/\S+/g);
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            word = _ref[_i];
            _results.push(writer({
              word: word,
              word2: word + (Math.floor(Math.random(100000000)))
            }));
          }
          return _results;
        });
        last_key = null;
        count = 0;
        return $.foreach_group(["word"], {
          add: ["count"]
        }, (function(keys, values, writer) {
          if (last_key !== keys.word) {
            last_key = keys.word;
            count = 0;
          }
          return count += 1;
        }), (function(keys, writer) {
          return writer({
            count: count
          });
        }));
      });
      return $.sink('input', $.tap("output", $.text_line_scheme("word", "word2")));
    });
  });

}).call(this);
