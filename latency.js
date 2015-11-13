var path = require('path');
var exec = require('child_process').exec;
var spawn = require('child_process').spawn;
var async = require('async');
var plist = require('plist');
var sconsole = require('sconsole');
var influent = require('influent');
var indent = require('indent-string');
var prettyjson = require('prettyjson');
var pkg = require('./package');

sconsole.setup({
    upto: sconsole.priority.info,
    ident: pkg.name,
    stdio: true,
    syslog: {
        upto: sconsole.priority.error
    }
});

var conf = {
    influx: {
        username : 'local',
        password : 'local',
        database : 'local',
        server   : {
            protocol : 'http',
            host     : 'localhost',
            port     : 8086
        }
    }
};

var options = {
    influx: null,
    disks: {}
};

async.series([
    function (next) {
        async.waterfall([
            getDevices,
            getDeviceUUIDs,
            printDevices
        ], next)
    },
    setupInflux,
    setupTrace
], end);

function setupInflux(done) {
    sconsole.info('Setup InfluxDB');
    influent
    .createHttpClient(conf.influx)
    .then(function (client) {
        options.influx = client;
        done(null);
    })
    .catch(function (error) {
        done(error);
    });
}

function setupTrace(done) {
    sconsole.info('Setup DTrace');
    var cp = spawn(path.resolve(__dirname, 'disklatency.d'));
    
    cp.on('error', cpError);
    cp.on('exit', cpExit);

    cp.stdout.on('readable', function () {
        read(cp.stdout).split('\n').forEach(cpData);
    });

    cp.stderr.on('readable', function () {
        cpError(new Error('Child process error:\n'+ read(cp.stderr)));
    });
    
    function read(stream) {
        var data;
        data = stream.read();
        data = data && data.toString().trim();
        return data;
    }
    
    done(null);
}

function cpError(error) {
    sconsole.error(error);
}

function cpExit(code) {
    if (code > 0) {
        sconsole.error('Child process exit code: '+ code);
    }
}

function cpData(data) {
    var disk, time, timestamp;
    
    data = data.split('\t');
    disk = data[0];
    disk = options.disks[disk].uuid;
    time = parseInt(data[1], 10);
    timestamp = Date.now() +'000000';
    
    options.influx.write({
        key: 'disk_latency',
        tags: {
            disk: disk
        },
        fields: {
            io_delta: time
        },
        timestamp: timestamp
    })
    .catch(cpError);
}

function getDevices(done) {
    sconsole.info('Get devices');
    var command = 'stat -f "%Hr-%Lr%t%N" /dev/disk*';
    exec(command, function (error, stdout, stderr) {
        var out = stdout;
        if (error) {
            done(error);
        } else {
            out = out
            .trim()
            .split('\n')
            .map(l => l.split('\t'))
            .filter(n => n[1].match(/s\d+$/))
            done(null, out);
        }
    });
}

function getDeviceUUIDs(disks, done) {
    sconsole.info('Get device UUIDs');
    async.map(disks, iterator, _done);
    
    function _done(error, disks) {
        if (error) {
            done(error);
        } else {
            disks = disks.filter(n => n.uuid && n.mount);
            disks.forEach(function (disk) {
                options.disks[disk.id] = disk;
            });
            done(null, disks);
        }
    }
    
    function iterator(disk, next) {
        var command = 'diskutil info -plist '+ disk[1];
        exec(command, function (error, stdout, stderr) {
            if (error) {
                next(error);
            } else {
                stdout = plist.parse(stdout);
                next(null, {
                    id    : disk[0],
                    uuid  : stdout.DiskUUID,
                    mount : stdout.MountPoint,
                    node  : stdout.DeviceNode
                });
            }
        });
    }
}

function end(error) {
    if (error) {
        sconsole.error(error);
        process.exit(1);
    }
}

function printDevices(disks, done) {
    var disks = options.disks;
    disks = Object.keys(disks).map(key => disks[key]);
    disks = prettyjson.render(disks);
    disks = indent(disks, ' ', 7);
    sconsole.info('\n'+ disks);
    done(null, disks);
}
