var zlib = require('zlib');
var path = require('path');
var Step = require('step');
var knox = require('knox');
var _ = require('underscore');
var argv = require('minimist')(process.argv.slice(2));
var geoip = new (require('geoip').City)(path.resolve(__dirname + '/support/GeoLiteCity.dat'));

var client = knox.createClient({
    key: argv.key,
    secret: argv.secret,
    token: argv.token,
    bucket: argv.bucket
});

// offset for systematic random sampling
var offset = argv.full ? 0 : Math.floor(Math.random() * 10) + 1;
var sample = [];
var pops = {};
var codes = {};
var cache = {};
var total = 0;
var cities = {};

process.on('uncaughtException', function(err) {
    // TODO Occasional EPIPE errors...
});

Step(function() {
    client.list({ prefix: argv.prefix }, this);
    }, function(err, data) {
        if (err) throw err;
        var group = this.group();
        // Random shuffle as part of systematic random sampling
        var results = _(data.Contents).shuffle();
        var count = results.length;
        // If not looking at all logs, look at 10%
        var increment = argv.full ? 1 : 10;
        for (var i = offset; i < count; i+=increment) {
            sample.push({
              key: results[i].Key,
              size: results[i].Size
            });
        }
        // Now fetch + parse all files
        sample.forEach(function(file) {
            fetch(file, group());
        });
    }, function(err) {
        if (err) throw err;

        _(cities).each(function(coords, ll) {
            var lat = ll.split('_')[0];
            var lon = ll.split('_')[1];
            var total = 0;
            _(cities[ll].times).each(function(time) {
                total+= time;
            });
            console.log(lat + ',' + lon + ',' + total / cities[ll].times.length);
        });
    }
);

// Fetch and parse a log file
function fetch(log, callback) {
    //Just look at the first 10% of the log file
    //var bytes = Math.ceil(log.size * 0.1);
    var bytes = log.size;
    var req = client.get('/' + log.key, {Range: 'bytes=0-' + bytes}).on('response', function(res) {
        var prevLine = false;
        var gunzip = zlib.createGunzip();
        res.pipe(gunzip);
        gunzip.on('data', function(data) {
            data = data.toString();
            if (prevLine) {
                data = prevLine + data;
                prevLine = false;
            }
            // If last char is not newline, a line got split b/t buffers
            var lastChar = data.slice(-1);
            var lines = data.split('\n');
            if (lastChar != '\n') prevLine = lines.pop();
            lines.forEach(function(line) {
                if (line.length) mapper(line);
            });
        });

        gunzip.on('end', function() {
            callback();
        });

    }).end();
}

// Parse a log entry
function mapper(line) {
    var parts = line.split(/\s+/g);
    if (parts.length > 5) {
        if (argv.log)
            console.log('"%s %s"', parts[5], parts[7]);
        else if (argv.geoip) {
            var edge = parts[2];
            var ip = parts[4];
            var time = parts[18];
            var status = parts[13];
            if (status == 'Miss') {
                geoip.lookup(ip, function(err, res) {
                    if (err) throw err;
                    var k = res.latitude + '_' + res.longitude;
                    if (!cities[k]) {
                        cities[k] = {};
                        cities[k].times = [];
                    }
                    cities[k].times.push(parseFloat(time));
                });
            }
        }
        else {
            total++;
            var pop = parts[2].substr(0, 3);
            var code = parts[8];
            var status = parts[13];
            if (!pops[pop]) pops[pop] = 1;
            else pops[pop]++;
            if (!codes[code]) codes[code] = 1;
            else codes[code]++;
            if (!cache[status]) cache[status] = 1;
            else cache[status]++;
        }
    }
}
