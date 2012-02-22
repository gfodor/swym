(function() {

  define(function() {
    return {
      TextLine: (function() {

        function _Class(fields) {
          this.fields = fields;
          console.log("new TextLine");
        }

        return _Class;

      })(),
      JsonLine: (function() {

        function _Class(fields) {
          this.fields = fields;
          console.log("new JsonLine");
        }

        return _Class;

      })()
    };
  });

}).call(this);
