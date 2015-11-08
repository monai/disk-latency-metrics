var path = require('path');
var exec = require('child_process').exec;
var spawn = require('child_process').spawn;
var async = require('async');
var plist = require('plist');
var minimist = require('minimist');
var influent = require('influent');
var argv = minimist(process.argv.slice(2), {
    alias: { u: 'uid', g: 'gid' },
    default: { }
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
    disks: null
};

async.series([
    getDevices,
    getDeviceUUIDs,
    setupInflux,
    setupTrace
], end);

function setupInflux(done) {
    influent
    .createClient(conf.influx)
    .then(function (client) {
        options.influx = client;
        done(null);
    })
    .catch(function (error) {
        done(error);
    });
}

function setupTrace(done) {
    var cp = spawn(path.resolve(__dirname, 'disklatency.d'));
    
    if (argv.gid) process.setgid(argv.gid);
    if (argv.uid) process.setuid(argv.uid);
    
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
    console.error(error);
}

function cpExit(code) {
    if (code > 0) {
        console.error('Child process exit code: '+ code);
    }
}

function cpData(data) {
    var disk, time;
    
    data = data.split('\t');
    disk = data[0] +'-'+ data[1];
    disk = options.disks[disk];
    time = data[2];
    
    options.influx.writeOne({
        key: disk,
        tags: {
            type: disk
        },
        fields: {
            time: time
        },
        timestamp: Date.now()
    })
    .catch(cpError);
}

function getDevices(done) {
    var command = 'stat -f "%N%t%Hr%t%Lr" /dev/disk*';
    exec(command, function (error, stdout, stderr) {
        if (error) {
            done(error);
        } else {
            stdout = stdout
            .trim()
            .split('\n')
            .map(l => l.split('\t'))
            .filter(n => n[0].match(/s\d+$/))
            .reduce(function (agg, curr) {
                var key = curr[1] +'-'+ curr[2];
                agg.push([key, curr[0]]);
                return agg;
            }, []);
            options.disks = stdout;
            done(null);
        }
    });
}

function getDeviceUUIDs(done) {
    async.map(options.disks, iterator, _done);
    
    function _done(error, disks) {
        if (error) {
            done(error);
        } else {
            disks = disks.filter(n => n[1]);
            options.disks = {};
            disks.forEach(function (disk) {
                options.disks[disk[0]] = disk[1];
            });
            
            done(null);
        }
    }
    
    function iterator(disk, next) {
        var command = 'diskutil info -plist '+ disk[1];
        exec(command, function (error, stdout, stderr) {
            if (error) {
                next(error);
            } else {
                stdout = plist.parse(stdout);
                next(null, [disk[0], stdout.DiskUUID]);
            }
        });
    }
}

function end(error) {
    if (error) {
        console.error(error);
        process.exit(1);
    }
}
