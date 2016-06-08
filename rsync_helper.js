"use strict"
var LineByLineReader = require('line-by-line');
var VSSSnapshots = require('vss-snapshot');
var HookScripts = require("hook-scripts");
var RsyncConf = require('rsync-conf');
var FileSystem = require('fs');
var Path = require('path');
var Registry = require('winreg');
var Q = require('q');

// Registry locations
const SettingsKey = '\\Software\\USNJBackupQueue';
const VolumesKey = SettingsKey + '\\Volumes';

// This creates an insance of a "path" object that will format things in Posix
//  format by default (which we need for any path that will be provided to
//  rsync)
var PosixPath = require('path').posix;

function GetSettings() {
  var deferred = Q.defer();
  var ReturnValues = {};

  var Key = new Registry({
    hive: Registry.HKLM,
    key: SettingsKey
  });

  Key.values(function(err, values) {
    for (var value of values) {
      AppSettings[value.name] = value.value;
    }

    deferred.resolve(true);
  })

  return deferred.promise;
}

/*
  Fetch a list of volumes that we should process based on the entries in the
  registry that are used by USNJBackupQueue.
*/
function EnumVolumes() {
  var deferred = Q.defer();
  var ReturnKeys = {};

  // Enumerate Volumes and input files that will need to be processed
  var Key = new Registry({
    hive: Registry.HKLM,
    key: VolumesKey
  });

  Key.keys(function(err, items) {
    if (err) {
      deferred.reject(err);
    }
    else {
      for (var index in items) {
        var item = items[index];
        //console.log(Path.basename(item.key));
        ReturnKeys[Path.basename(item.key)] = false;
        item.values(function(err, values) {

          // Turn all of the returned items into simple name -> value pairs.
          ReturnKeys[Path.basename(item.key)] = '';
          var ReturnValues = {};
          for (var index in values) {
            //console.log(values[index].name);
            ReturnValues[values[index].name] = values[index].value;
          }
          ReturnKeys[Path.basename(item.key)] = ReturnValues;

          // If all of the item.values callbacks have happened, then none of the
          //  ReturnKeys values will be "false" any more.  This means that all
          //  data is loaded and we can resolve the promise.
          //TODO: Q.allSettled is the smarter way to do this
          var AllFilled = true;
          for (var index in ReturnKeys) {
            if (ReturnKeys[index] == false) {
              var AllFilled = false;
            }
          }
          if (AllFilled) {
            deferred.resolve(ReturnKeys);
          }
        });
      }
      //deferred.resolve(items);
    }
  });
  return deferred.promise;
}

function ProcessFile(InputFilename, OutputFilename) {
  // Are are going to wedge each file/directory into an object using the full
  //  path as the index.  This is a dodgy, memory consuming way to dedup the
  //  list.
  //  TODO: replace me with something more efficient.
  var OutputLines = {};

  var deferred = Q.defer();

  // Start reading the file
  var LineReader = new LineByLineReader(InputFilename, {skipEmptyLines: true});

  // IF something fails, fail the promise
  LineReader.on('error', function(err) {
    console.log(err);
    deferred.fail(err);
  });

  LineReader.on('line', function(line) {
    // Pause the generation of "line" events while we process this line
    LineReader.pause();

    // Process the line
    var ThisFile = Path.parse(line);
    var ThisFileName = PosixPath.format(ThisFile);

    // If this line already exists in the OutputLines array, don't process it
    //  again
    if (OutputLines[ThisFileName]) {
      //TODO: Add a setTimeout to limit CPU usage?
      LineReader.resume();
    }

    else {
      var NewLines = "+ /" + ThisFileName + '\r\n';
      OutputLines[PosixPath.format(ThisFile)] = 1;

      // rsync needs not just the files we want to backup in the resulting
      //  include-from list but also the parent directories.  Recurse up to the
      //  highest possible level and include all of those directories.
      var ThisDir = Path.parse(ThisFile.dir);
      var ThisDirName = PosixPath.format(ThisDir);
      while (ThisDir.name.length > 0) {
        if (!OutputLines[ThisFileName]) {
          OutputLines[ThisDirName] = 1;
          NewLines += "+ /" + ThisDirName + '\r\n';
        }

        ThisDir = Path.parse(ThisDir.dir);
        ThisDirName = PosixPath.format(ThisDir);
      }

      // Use the callback from the appendFile function as the trigger to ask the
      //  LineReader for the next line
      FileSystem.appendFile(OutputFilename, NewLines, function() {
        //TODO: Add a setTimeout to limit CPU usage?
        LineReader.resume();
      })
    }
  });

  LineReader.on('end', function() {
    // The last thing we need to do before returning our promise is add the
    //  "exclude everything else" entry to the end of the file.
    FileSystem.appendFile(OutputFilename, "- *\r\n", function() {
      deferred.resolve(OutputLines);
    })
  })

  return deferred.promise;
}

// Main script entry point
//  This would be simplier if rewritten with generators/yield but I'll get to
//  know promises better first.
let Volumes = {};
let VolumeSnapshots = {};
let VolumeSuitableSnapshots = {};

let AppSettings = {
  // Cygwin default settings
  'RsyncConfPath': 'C:\\cygwin\\etc\\rsyncd.conf',
  'RsyncLockPath': '/var/run/rsyncd.lock',
  'RsyncLogPath': '/var/log/rsyncd.log',
  'RsyncdServiceName': 'rsyncd',

  // Other default setting values
  'MaxUsableSnapshotAge': 3600
};

Q.allSettled([EnumVolumes(), GetSettings()])
.then(function(items) {
  Volumes = items[0].value;
  //AppSettings = items[1].value;

  console.log(AppSettings);

  // Override the HookScripts.SearchPaths setting if one was specified in the
  //  registry.
  if (AppSettings['HookScriptsDir']) {
    HookScripts.SearchPaths = [ AppSettings['HookScriptsDir'] ];
  }

  //TODO: Execute any pre-snapshot hook scripts
  return Q.allSettled([HookScripts.RunScripts("pre-snapshot")]);
})

.then(function(items) {
  //Create a list of VSSSnapshots that already exist
  var VSSListPromises = [];
  for (let index in Volumes) {
    let Volume = index;
    VSSListPromises.push(VSSSnapshots.List(Volume));
  }
  return Q.allSettled(VSSListPromises);
})

.then(function(items) {
  // Process the results from the VSSSnapshots.List operation into a single array
  for (let index in items) {
    // Handle any errors
    if (items[index].state == 'rejected') {
      throw items[index].reason;
    }

    for (let snapshot of items[index].value) {
      if (!VolumeSnapshots[snapshot.Volume]) {
        VolumeSnapshots[snapshot.Volume] = [];
      }
      VolumeSnapshots[snapshot.Volume].push(snapshot);
    }
  }
  console.log(VolumeSnapshots);

  // If a snapshot was created recently, use it.  Otherwise, create a new
  //  snapshot now
  let VolumeCreatePromises = [];
  for (let index in Volumes) {
    for (let snapshot of VolumeSnapshots[index]) {
      // Only accept snapshots that are less than "MaxUsableSnapshotAge" seconds
      //  old.
      if (snapshot.Age() < AppSettings['MaxUsableSnapshotAge']) {
        if (VolumeSuitableSnapshots[index]) {
          if (VolumeSuitableSnapshots[index].Age() > snapshot.Age()) {
            VolumeSuitableSnapshots[index] = snapshot;
          }
        }
        else {
          VolumeSuitableSnapshots[index] = snapshot;
        }
      }
    }
    if (!VolumeSuitableSnapshots[index]) {
      console.log("No suitable existing snapshot of " + index + " exists.  A new snapshot will be created.");
      VolumeCreatePromises.push(VSSSnapshots.Create(index));
    }
    else {
      console.log("Using existing snapshot of " + index + " created " + VolumeSuitableSnapshots[index].Age() + "s ago");
    }
  }

  if (VolumeCreatePromises.length > 0) {
    return Q.allSettled(VolumeCreatePromises);
  }
  else {
    // This is just a convienient way to pass onto the next .then().  Probably a
    //  prettier way.
    let deferred = Q.defer();
    setTimeout(function() {
      deferred.resolve();
    }, 1);
    return Q.allSettled(deferred);
  }
})

.then(function(items) {
  for (let index in items) {
    // Handle any errors
    if (items[index].state == 'rejected') {
      throw items[index].reason;
    }

    // Add any snapshots in "items" to the "VolumeSnapshots" array and select
    //  the new snapshot as the most suitable for rsyncing from.
    try {
      if (items[index].value instanceof VSSSnapshots.VSSSnapshot) {
        console.log("A new snapshot was created.");
        VolumeSnapshots[items[index].value.Volume].push(items[index].value);
        VolumeSuitableSnapshots[items[index].value.Volume] = items[index].value;
      }
    }
    catch (err) { console.log(err) }
  }

  //TODO: Execute any post-snapshot hook scripts
  return Q.allSettled([HookScripts.RunScripts("post-snapshot")]);
})

.then(function(items) {
  // Extract the OutputToFile values for each volume and start a ProcessFile()
  var ProcessFilePromises = [];
  for (let index in Volumes) {
    var InputFilename = Volumes[index]['OutputToFile'];

    // The output file is going to be the same path as the input file only
    //  without the .in at the end.
    //TODO: Make the OutputFilename customisable from the registry (same key as
    //  read by EnumVolumes)
    var OutputFilename = Path.parse(InputFilename);
    OutputFilename.ext = "";
    OutputFilename.base = "";
    OutputFilename = Path.format(OutputFilename);

    console.log(InputFilename);
    console.log(OutputFilename);
    ProcessFilePromises.push(ProcessFile(InputFilename, OutputFilename));
  }

  // Only return from this "then" block once all of the ProcessFile promises are
  //  settled.
  return Q.allSettled(ProcessFilePromises);
})
.then(function() {
  // Configure rsync daemon
  RsyncConf.MaxConnections = 2;
  RsyncConf.UseChroot = false;
  RsyncConf.LogFile = AppSettings['RsyncLogPath'];
  RsyncConf.LockFile = AppSettings['RsyncLockPath'];

  for (let index in Volumes) {
    let VolumeDriveLetter = index.replace(/:/, '');

    // Reformat the snapshot's path to posix slashes and cygwin prefixed
    if (VolumeSuitableSnapshots[index]) {
      if (VolumeSuitableSnapshots[index] instanceof VSSSnapshots.VSSSnapshot) {
        let ModulePath = Path.parse(VolumeSuitableSnapshots[index].Path);
        ModulePath = PosixPath.format(ModulePath);
        ModulePath = ModulePath.replace(/\\\\\?\\GLOBALROOT\\/, '/proc/sys/');

        // This looks stupid but it's necessary for rsync to use the snapshot
        //  properly
        ModulePath += '/\\./'

        let NewModule = new RsyncConf.Module("VSS" + VolumeDriveLetter, ModulePath);
        NewModule.ReadOnly = true;
        NewModule.List = false;
        NewModule.IncludeFrom = 'testing';
        RsyncConf.AddModule(NewModule);
      }
      else {
        console.log("WARNING: The snapshot selected for " + index + " isn't actaully a snapshot.  This should never happen!");
      }
    }
    else {
      console.log("WARNING: No suitable snapshot of " + index + " were found or created.  This should never happen!");
    }
  }
  //console.log(RsyncConf.toString());

  // Save the config and restart the service (the final argument is the
  //  customised service name)
  return RsyncConf.Save(AppSettings['RsyncConfPath'], AppSettings['RsyncdServiceName']);
})
.then(function() {
  // We're all done?
})
.fail(function(err) {
  console.log("Promise Error: ", err.message);
})
.done();
