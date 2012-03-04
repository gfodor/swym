#insert - declare new columns
#replace - declare replacement map
#remove - declare removal list
# pipe argument selector is whole set

listing_fields = [ "listing_id", "state", "user_id", "title", "description", "creation_tsz",
  "ending_tsz", "original_creation_tsz", "last_modified_tsz", "price", "currency_code", "quantity",
  "tags", "materials", "section_id", "featured_rank", "views", "image_listing_id",
  "state_tsz", "last_modified_tsz_epoch", "saturation", "brightness", "is_black_and_white"
]

require { baseUrl: "lib/js" }, ["cascading/builder", "cascading/schemes", "underscore"], (builder, schemes, _) ->
  with_test_flow = (f) ->
    cascade = builder.cascade ($) ->
      $.flow 'word_counter', ->
        f($)

  expect_bad_flow = (msg, f) ->
    expect(-> with_test_flow(f)).toThrow(new Error(msg))

  describe "job builder", ->
    it "should set outgoing scope on tap", ->
      with_test_flow ($) ->
        tap = $.source 'input', $.tap("listings.txt", $.text_line_scheme())
        expect(tap.outgoing).toEqual ["line"]

      with_test_flow ($) ->
        tap = $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line"))
        expect(tap.outgoing).toEqual ["offset", "line"]

    it "should fail if trying to tap bad source name", ->
      expect_bad_flow "Unknown source bad_input for assembly", ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1"))

        a1 = $.assembly 'bad_input', ->

        $.sink 'input', $.tap("output", $.text_line_scheme())

    it "should set incoming scope on assembly", ->
      with_test_flow ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1"))
        $.source 'input_2', $.tap("listings.txt", $.text_line_scheme("line_2"))

        a1 = $.assembly 'input', ->
        a2 = $.assembly 'input_2', ->

        expect(a1.head_pipe.incoming).toEqual ["offset", "line_1"]
        expect(a1.head_pipe.outgoing).toEqual ["offset", "line_1"]
        expect(a2.head_pipe.incoming).toEqual ["line_2"]
        expect(a2.head_pipe.incoming).toEqual ["line_2"]

    it "should fail if no sinks", ->
    it "should fail if no assembly for sink", ->
    it "should fail if duplicate assembly", ->
      expect_bad_flow "Duplicate assembly input", ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1"))

        a1 = $.assembly 'input', ->
        a1 = $.assembly 'input', ->

        $.sink 'input', $.tap("output", $.text_line_scheme())

    it "should fail if duplicate source", ->
      expect_bad_flow "Duplicate source input", ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1"))
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1"))

        a1 = $.assembly 'input', ->

        $.sink 'input', $.tap("output", $.text_line_scheme())

    it "should fail if duplicate sink", ->
      expect_bad_flow "Duplicate sink input", ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line_1"))

        a1 = $.assembly 'input', ->

        $.sink 'input', $.tap("output", $.text_line_scheme())
        $.sink 'input', $.tap("output", $.text_line_scheme())

    it "should fail if any unbound sinks", ->
    it "should fail if any unbound sources", ->

    it "should propagate fields correctly for map spec", ->
      with_test_flow ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line"))

        $.assembly 'input', ->
          insert_step = $.map { add: ["upcase", "line_number"], remove: ["offset", "line"] }, (tuple, writer) ->
            writer({ upcase: line.toUpperCase(), line_number: tuple.offset })

          expect(insert_step.incoming.sort()).toEqual ["line", "offset"].sort()
          expect(insert_step.outgoing.sort()).toEqual ["line_number", "upcase"].sort()

    it "should exception if trying to rename an invalid field", ->
      expect_bad_flow "Invalid field bogus being removed", ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line"))

        $.assembly 'input', ->
          $.map { remove: ["bogus"] }, (tuple, writer) ->
            writer({ upcase: line.toUpperCase() })

    it "should exception if bad spec argument", ->
      expect_bad_flow "Invalid argument bogus for field spec", ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line"))

        $.assembly 'input', ->
          $.map { bogus: ["line"] }, (tuple, writer) ->
            writer({ upcase: line.toUpperCase() })

    it "should connect each pipe correctly", ->
      with_test_flow ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line"))

        step = null

        assembly = $.assembly 'input', ->
          step = $.map { add: ["foo"] }, (tuple, writer) ->
            tuple.foo = "bar"
            writer(tuple)

        expect(assembly.tail_pipe.pipe_id).toEqual(step.each.pipe_id)
        expect(assembly.head_pipe.pipe_id).toEqual(step.each.parent_pipe.pipe_id)

    it "should generate a correct processor function", ->
      with_test_flow ($) ->
        $.source 'input', $.tap("listings.txt", $.text_line_scheme("offset", "line"))

        $.assembly 'input', ->
          $.map { add: ["word"], remove: ["line"] }, (tuple, writer) ->
            for word in tuple.line.match(/\S+/g)
              writer({ word: word })

          $.map { }, (tuple, writer) ->
            tuple.word = tuple.word.toUpperCase()
            writer(tuple)

          final_map = $.map { add: "word_copy" }, (tuple, writer) ->
            tuple.word_copy = tuple.word
            writer(tuple)

          output = []

          final_map.each.processor { line: "hello world" }, (out) ->
            output[output.length] = out

          expect(output.length).toEqual(2)
          expect(output[0].line).toBeUndefined()
          expect(output[0].word).toEqual("HELLO")
          expect(output[0].word_copy).toEqual("HELLO")

          expect(output[1].line).toBeUndefined()
          expect(output[1].word).toEqual("WORLD")
          expect(output[1].word_copy).toEqual("WORLD")
