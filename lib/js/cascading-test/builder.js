(function() {
  var listing_fields;

  listing_fields = ["listing_id", "state", "user_id", "title", "description", "creation_tsz", "ending_tsz", "original_creation_tsz", "last_modified_tsz", "price", "currency_code", "quantity", "tags", "materials", "section_id", "featured_rank", "views", "image_listing_id", "state_tsz", "last_modified_tsz_epoch", "saturation", "brightness", "is_black_and_white"];

  require({
    baseUrl: "lib/js"
  }, ["cascading/builder", "cascading/schemes", "cascading/util", "underscore"], function(builder, schemes, U, _) {
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
          tap = $.source('input', $.tap("listings.txt", $.text_line_scheme()));
          expect(tap.outgoing).toEqual(["line"]);
          return expect(tap.outgoing_types.line).toEqual(U.type_idx_map.string);
        });
        return with_test_flow(function($) {
          var tap;
          tap = $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line")));
          expect(tap.outgoing).toEqual(["offset", "line"]);
          expect(tap.outgoing_types.offset).toEqual(U.type_idx_map.int);
          return expect(tap.outgoing_types.line).toEqual(U.type_idx_map.string);
        });
      });
      it("should fail if trying to tap bad source name", function() {
        return expect_bad_flow("Unknown source bad_input for assembly", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1")));
          a1 = $.assembly('bad_input', function() {});
          return $.sink('input', $.tap("output", $.text_line_scheme()));
        });
      });
      it("should set incoming scope on assembly", function() {
        return with_test_flow(function($) {
          var a1, a2;
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1")));
          $.source('input_2', $.tap("listings.txt", $.text_line_scheme("line_2")));
          a1 = $.assembly('input', function() {});
          a2 = $.assembly('input_2', function() {});
          expect(a1.head_pipe.incoming).toEqual(["offset", "line_1"]);
          expect(a1.head_pipe.incoming_types.offset).toEqual(U.type_idx_map.int);
          expect(a1.head_pipe.incoming_types.line_1).toEqual(U.type_idx_map.string);
          expect(a1.head_pipe.outgoing).toEqual(["offset", "line_1"]);
          expect(a1.head_pipe.outgoing_types.offset).toEqual(U.type_idx_map.int);
          expect(a1.head_pipe.outgoing_types.line_1).toEqual(U.type_idx_map.string);
          expect(a2.head_pipe.incoming).toEqual(["line_2"]);
          return expect(a2.head_pipe.incoming_types.line_2).toEqual(U.type_idx_map.string);
        });
      });
      it("should fail if no sinks", function() {});
      it("should verify arity of map function", function() {});
      it("should verify arity of foreach_group function", function() {});
      it("should fail if no assembly for sink", function() {});
      it("should fail if invalid type info for a field", function() {
        return expect_bad_flow("Invalid type foo for count", function($) {
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line")));
          return $.assembly('input', function() {
            return $.foreach_group(["line"], {
              add: {
                count: "foo"
              }
            });
          });
        });
      });
      it("should fail if duplicate assembly", function() {
        return expect_bad_flow("Duplicate assembly input", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1")));
          a1 = $.assembly('input', function() {});
          a1 = $.assembly('input', function() {});
          return $.sink('input', $.tap("output", $.text_line_scheme()));
        });
      });
      it("should fail if duplicate source", function() {
        return expect_bad_flow("Duplicate source input", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1")));
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1")));
          a1 = $.assembly('input', function() {});
          return $.sink('input', $.tap("output", $.text_line_scheme()));
        });
      });
      it("should fail if duplicate sink", function() {
        return expect_bad_flow("Duplicate sink input", function($) {
          var a1;
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1")));
          a1 = $.assembly('input', function() {});
          $.sink('input', $.tap("output", $.text_line_scheme()));
          return $.sink('input', $.tap("output", $.text_line_scheme()));
        });
      });
      it("should fail if any unbound sinks", function() {});
      it("should fail if any unbound sources", function() {});
      it("should propagate fields correctly for map spec", function() {
        return with_test_flow(function($) {
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line")));
          return $.assembly('input', function() {
            var insert_step;
            insert_step = $.map({
              add: {
                upcase: "string",
                line_number: "int"
              },
              remove: ["offset", "line"]
            }, function(tuple, writer) {
              return writer({
                upcase: tuple.line.toUpperCase(),
                line_number: tuple.offset
              });
            });
            expect(insert_step.incoming.sort()).toEqual(["line", "offset"].sort());
            expect(insert_step.outgoing.sort()).toEqual(["line_number", "upcase"].sort());
            expect(insert_step.incoming_types.offset).toEqual(U.type_idx_map.int);
            expect(insert_step.incoming_types.line).toEqual(U.type_idx_map.string);
            expect(insert_step.outgoing_types.upcase).toEqual(U.type_idx_map.string);
            return expect(insert_step.outgoing_types.line_number).toEqual(U.type_idx_map.int);
          });
        });
      });
      it("should exception if trying to rename an invalid field", function() {
        return expect_bad_flow("Invalid field bogus being removed", function($) {
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line")));
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
      it("should exception if bad spec argument", function() {
        return expect_bad_flow("Invalid argument bogus for field spec", function($) {
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line")));
          return $.assembly('input', function() {
            return $.map({
              bogus: ["line"]
            }, function(tuple, writer) {
              return writer({
                upcase: line.toUpperCase()
              });
            });
          });
        });
      });
      it("should connect each pipe correctly", function() {
        return with_test_flow(function($) {
          var assembly, step;
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line")));
          step = null;
          assembly = $.assembly('input', function() {
            return step = $.map({
              add: {
                foo: "string"
              }
            }, function(tuple, writer) {
              tuple.foo = "bar";
              return writer(tuple);
            });
          });
          expect(assembly.tail_pipe.pipe_id).toEqual(step.each.pipe_id);
          return expect(assembly.head_pipe.pipe_id).toEqual(step.each.parent_pipe.pipe_id);
        });
      });
      it("should generate a correct processor function", function() {
        return with_test_flow(function($) {
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line")));
          return $.assembly('input', function() {
            var final_map, output;
            $.map({
              add: {
                word: "string"
              },
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
              add: {
                word_copy: "string"
              }
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
      it("should raise error if grouping fields are invalid", function() {});
      it("should raise error if sorting fields are invalid", function() {});
      it("should raise error if grouping fields contains duplicates", function() {});
      return it("should connect foreach_group correctly", function() {
        return with_test_flow(function($) {
          var first, pipe;
          $.source('input', $.tap("listings.txt", $.text_line_scheme("offset", "line")));
          pipe = null;
          first = null;
          $.assembly('input', function() {
            first = $.map({
              add: {
                word: "string"
              },
              remove: ["line"]
            }, function(tuple, writer) {
              return writer({
                word: tuple.line
              });
            });
            return pipe = $.foreach_group(["word"], {
              add: {
                count: "int"
              }
            }, (function(keys, values, writer) {}), (function(keys, writer) {}));
          });
          expect(pipe.is_group_by != null).toEqual(true);
          expect(pipe.incoming.length).toEqual(2);
          expect(pipe.incoming_types.word).toEqual(U.type_idx_map.string);
          expect(pipe.incoming_types.offset).toEqual(U.type_idx_map.int);
          expect(pipe.outgoing.length).toEqual(2);
          expect(pipe.outgoing[0]).toEqual("word");
          expect(pipe.outgoing[1]).toEqual("count");
          expect(pipe.outgoing_types.word).toEqual(U.type_idx_map.string);
          expect(pipe.outgoing_types.count).toEqual(U.type_idx_map.int);
          return expect(pipe.parent_pipe.pipe_id).toEqual(first.each.pipe_id);
        });
      });
    });
  });

}).call(this);
