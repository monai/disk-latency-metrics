var path = require('path');
var exec = require('child_process').exec;
var spawn = require('child_process').spawn;
var rc = require('rc');
var async = require('async');
var plist = require('plist');
var sconsole = require('sconsole');
var influent = require('influent');
var indent = require('indent-string');
var prettyjson = require('prettyjson');
var pkg = require('./package');

var conf = rc(pkg.name, {
    devices: '/dev/disk*',
    influx: {
        username : 'local',
        password : 'local',
        database : 'local',
        server   : {
            protocol : 'http',
            host     : 'localhost',
            port     : 8086
        }
    },
    sconsole: {
        upto: sconsole.priority.info,
        ident: pkg.name,
        stdio: process.stdin.isTTY,
        syslog: {
            upto: sconsole.priority.error
        }
    }
});

sconsole.setup(conf.sconsole);

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
    
    cp.on('error', sconsole.error);
    cp.on('exit', cpExit);

    cp.stdout.on('readable', function () {
        read(cp.stdout).split('\n').forEach(cpData);
    });

    cp.stderr.on('readable', function () {
        sconsole.error(new Error('Child process error:\n'+ read(cp.stderr)));
    });
    
    function read(stream) {
        var data;
        data = stream.read();
        data = data && data.toString().trim();
        return data;
    }
    
    done(null);
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
    disk = options.disks[disk];
    disk = disk && disk.uuid;
    time = parseInt(data[1], 10);
    timestamp = Date.now() +'000000';
    
    if ( ! disk) {
        sconsole.error(new Error('Unknown disk: '+ data[0]))
    } else {
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
        .catch(sconsole.error);
    }
}

function getDevices(done) {
    sconsole.info('Get devices');
    var command = 'stat -f "%Hr-%Lr%t%N" '+ conf.devices;
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
