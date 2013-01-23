var args = require("optimist").argv;
var optional = require("optional");
var mongoose = require("mongoose");
var config = optional("./.config") || require("./config");
var request = require("request");
var csv = require("csv");
var fs = require("fs");

if(args.h){
  console.log("\nCollection engine for stock symbols, usage:");
  console.log("\t--refreshlist\tWill print a fresh list of symbols out for under -x $");
  console.log("\t-x\t\tWill override the default filter of $10, use with --refreshlist");
  console.log("\n");
  process.exit(0);
}

if(args.refreshlist){
  var symbols = {s:"",c:0};
  var input = fs.createReadStream(__dirname + "/symbols.csv");
  csv().from.stream(input,{delimiter:"\t"}).on("record",function(){
    symbols.s += arguments[0][0] + "+";
    symbols.c++;
    if(symbols.c > 100){
      var url = "http://finance.yahoo.com/d/quotes.csv?s=" + symbols.s + "&f=nsb2";
      symbols.s = "";
      symbols.c = 0;
      request(url,function(){
        var body = arguments[2];
        csv().from(body).on("record",function(){
          var val = parseFloat(arguments[0][2]);
          if(val < (args.x || 10)){
            console.log(arguments[0][1]);
          }
        });
      });
    }
  });
}

var collect = function(){
  mongoose.connect(config.db);
  var format = mongoose.model("data",mongoose.Schema({
    symbol:String
    ,open:Number
    ,close:Number
    ,bid:Number
    ,ask:Number
    ,volume:Number
    ,datetime:Date
  }));
  var input = fs.createReadStream(__dirname + "/under10.csv");
  var symbols = {s:"",c:0};
  var datetime = new Date();
  var nNaN = function(num,def){ return isNaN(parseFloat(num)) ? def || null : parseFloat(num); };
  var req = function(disconnect){
    var url = "http://finance.yahoo.com/d/quotes.csv?s=" + symbols.s + "&f=sopb3b2v";
    symbols.s = "";
    symbols.c = 0;
    request(url,function(e,r,b){
      var body = arguments[2];
      e && console.log("E::" + e);
      csv().from(body).on("record",function(data){
        var d = new format();
        console.log("FOUND::"+data[0]);
        d.symbol = data[0];
        d.open = nNaN(data[1]);
        d.close = nNaN(data[2]);
        d.bid = nNaN(data[3]);
        d.ask = nNaN(data[4]);
        d.volume = nNaN(data[5]);
        d.datetime = datetime;
        d.save();
      }).on("error",function(data){
        console.log("PE: " + data);
      }).on("end",function(){
      });
      console.log("REQLEN: " + body.length);
    });
  };
  csv().from.stream(input).on("record",function(){
    symbols.s += arguments[0][0] + "+";
    symbols.c++;
    if(symbols.c > 100){
      req();
    }
  }).on("end",function(count){
    req(true);
  });
};

if(args.collect && !args.refreshlist){
  collect();
}

if(process.env.PERMCOLLECT){
  collect();
  setInterval(collect, 5*60*1000); //collect every 5 minutes
}
