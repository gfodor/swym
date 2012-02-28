(function() {
  var listing_fields;

  listing_fields = ["listing_id", "state", "user_id", "title", "description", "creation_tsz", "ending_tsz", "original_creation_tsz", "last_modified_tsz", "price", "currency_code", "quantity", "tags", "materials", "section_id", "featured_rank", "views", "image_listing_id", "state_tsz", "last_modified_tsz_epoch", "saturation", "brightness", "is_black_and_white"];

  require({
    baseUrl: "lib/js"
  }, ["cascading/builder", "cascading/schemes", "underscore"], function(builder, schemes, _) {
    var expect_bad_flow, with_test_flow;
    with_test_flow = function(f) {
      var cascade;
      return cascade = builder.cascade(function($) {
        return $.flow('word_counter', function() {
          return f($);
        });
      });
    };
    expect_bad_flow = function(msg, f) {
      return expect(function() {
        return with_test_flow(f);
      }).toThrow(new Error(msg));
    };
    return describe("job builder", function() {
      it("should set outgoing scope on tap", function() {
        with_test_flow(function($) {
          var tap;
          tap = $.source('input', $.tap("listings.txt", new schemes.TextLine()));
          return expect(tap.outgoing).toEqual(["line"]);
        });
        return with_test_flow(function($) {
          var tap;
          tap = $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line")));
          return expect(tap.outgoing).toEqual(["offset", "line"]);
        });
      });
      it("should fail if trying to tap bad source name", function() {
        return expect_bad_flow("Unknown source bad_input for assembly", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1")));
          a1 = $.assembly('bad_input', function() {});
          return $.sink('input', $.tap("output", new schemes.TextLine()));
        });
      });
      it("should set incoming scope on assembly", function() {
        return with_test_flow(function($) {
          var a1, a2;
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1")));
          $.source('input_2', $.tap("listings.txt", new schemes.TextLine("line_2")));
          a1 = $.assembly('input', function() {});
          a2 = $.assembly('input_2', function() {});
          expect(a1.head_pipe.incoming).toEqual(["offset", "line_1"]);
          expect(a1.head_pipe.outgoing).toEqual(["offset", "line_1"]);
          expect(a2.head_pipe.incoming).toEqual(["line_2"]);
          return expect(a2.head_pipe.incoming).toEqual(["line_2"]);
        });
      });
      it("should fail if no sinks", function() {});
      it("should fail if no assembly for sink", function() {});
      it("should fail if duplicate assembly", function() {
        return expect_bad_flow("Duplicate assembly input", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1")));
          a1 = $.assembly('input', function() {});
          a1 = $.assembly('input', function() {});
          return $.sink('input', $.tap("output", new schemes.TextLine()));
        });
      });
      it("should fail if duplicate source", function() {
        return expect_bad_flow("Duplicate source input", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1")));
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1")));
          a1 = $.assembly('input', function() {});
          return $.sink('input', $.tap("output", new schemes.TextLine()));
        });
      });
      it("should fail if duplicate sink", function() {
        return expect_bad_flow("Duplicate sink input", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1")));
          a1 = $.assembly('input', function() {});
          $.sink('input', $.tap("output", new schemes.TextLine()));
          return $.sink('input', $.tap("output", new schemes.TextLine()));
        });
      });
      it("should fail if any unbound sinks", function() {});
      it("should fail if any unbound sources", function() {});
      it("should fail if TextLine doesn't have 0, 1, 2 fields", function() {
        return expect_bad_flow("TextLine can only have at most two fields (one for offset, one for line)", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line_1", "line_2")));
          a1 = $.assembly('input', function() {});
          return $.sink('input', $.tap("output", new schemes.TextLine()));
        });
      });
      it("should propagate fields correctly for map spec", function() {
        return with_test_flow(function($) {
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line")));
          return $.assembly('input', function() {
            var insert_step;
            insert_step = $.map({
              add: ["upcase", "line_number"],
              remove: ["offset", "line"]
            }, function(tuple, writer) {
              return writer({
                upcase: line.toUpperCase(),
                line_number: tuple.offset
              });
            });
            expect(insert_step.incoming.sort()).toEqual(["line", "offset"].sort());
            return expect(insert_step.outgoing.sort()).toEqual(["line_number", "upcase"].sort());
          });
        });
      });
      it("should exception if trying to rename an invalid field", function() {
        return expect_bad_flow("Invalid field bogus being removed", function($) {
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line")));
          return $.assembly('input', function() {
            return $.map({
              remove: ["bogus"]
            }, function(tuple, writer) {
              return writer({
                upcase: line.toUpperCase()
              });
            });
          });
        });
      });
      return it("should generate a correct processor function", function() {
        return with_test_flow(function($) {
          $.source('input', $.tap("listings.txt", new schemes.TextLine("offset", "line")));
          return $.assembly('input', function() {
            var final_map, output;
            $.map({
              add: ["word"],
              remove: ["line"]
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
            final_map = $.map({
              add: "word_copy"
            }, function(tuple, writer) {
              tuple.word_copy = tuple.word;
              return writer(tuple);
            });
            output = [];
            final_map.each.processor({
              line: "hello world"
            }, function(out) {
              return output[output.length] = out;
            });
            expect(output.length).toEqual(2);
            expect(output[0].line).toBeUndefined();
            expect(output[0].word).toEqual("HELLO");
            expect(output[0].word_copy).toEqual("HELLO");
            expect(output[1].line).toBeUndefined();
            expect(output[1].word).toEqual("WORLD");
            return expect(output[1].word_copy).toEqual("WORLD");
          });
        });
      });
    });
  });

}).call(this);
