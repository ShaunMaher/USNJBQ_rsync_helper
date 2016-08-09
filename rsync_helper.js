"use strict"
var LineByLineReader = require('line-by-line');
var VSSSnapshots = require('vss-snapshot');
var HookScripts = require("hook-scripts");
var RsyncConf = require('rsync-conf');
var FileSystem = require('fs');
var Path = require('path');
var Registry = require('winreg');
var CycleLogs = require("cycle-logs");
var Q = require('q');

// Registry locations
const SettingsKey = '\\Software\\USNJBackupQueue';
const VolumesKey = SettingsKey + '\\Volumes';

// This creates an insance of a "path" object that will format things in Posix
//  format by default (which we need for any path that will be provided to
//  rsync)
var PosixPath = require('path').posix;

function SaveNewSnapshot(Volume, Snapshot) {
  var deferred = Q.defer();
  var RunningPromises = [];
  var SnapshotID = '';

  if (Snapshot instanceof VSSSnapshots.VSSSnapshot) {
    SnapshotID = Snapshot.ID;
  }
  else {
    SnapshotID = Snapshot;
  }

  var Key = new Registry({
    hive: Registry.HKLM,
    key: VolumesKey + '\\' + Volume
  });

  Key.get("SnapshotsCreated", function(err, value) {
    let CurrentValue = '';

    // I'm going to support both REG_SZ and REG_MULTI_SZ types here, defaulting
    //  to REG_SZ.  The winreg module doesn't seem to support REG_MULTI_SZ right
    //  now but might in the future.
    let KeyType = Registry.REG_SZ;

    // Get the current value
    if (value) {
      CurrentValue = value.value;
      KeyType = value.type;
    }

    // Append the new value based on the current value type
    let NewValue = '';
    if (KeyType == Registry.REG_SZ) {
      NewValue = CurrentValue + ',' + SnapshotID + ',';
      NewValue = NewValue.replace(/^,/g, '');
      NewValue = NewValue.replace(/,$/g, '');
      NewValue = NewValue.replace(/,,/g, ',');
    }
    else if (KeyType == Registry.REG_MULTI_SZ) {
      NewValue = CurrentValue + ',\n' + SnapshotID + '\n';
      NewValue.replace(/\n\n/, "\n");
    }
    else {
      // No other types are supported.  Value must be broken in some way.
      NewValue = SnapshotID;
      KeyType = Registry.REG_SZ;
    }

    Key.set("SnapshotsCreated", KeyType, NewValue, function(err) {
      if (err) {
        console.log(err);
        deferred.fail(err);
        return false;
      }
      else {
        return deferred.resolve(true);
        return true;
      }
    });
  });

  return deferred.promise;
}

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

function ProcessFile(InputFilename, IntermediateFilename, OutputFilename) {
  // Are are going to wedge each file/directory into an object using the full
  //  path as the index.  This is a dodgy, memory consuming way to dedup the
  //  list.
  //  TODO: replace me with something more efficient.
  var OutputLines = {};

  var deferred = Q.defer();

  FileSystem.stat(InputFilename, function(err, stats) {
    if (err) {
      // If the InputFilename wasn't created, it might be because there were no
      //  new lines since the most recent backup.  This is not an error.
      if (err.code == 'ENOENT') {
        console.log("No new lines found?");
        deferred.resolve(OutputLines);
      }
      else {
        console.log(err);
        deferred.fail(err);
      }
      return;
    }

    // First up we need to append the content of InputFilename to the
    //  IntermediateFilename.  If a backup wasn't run between the last time we
    //  processed the InputFilename and now, or the backup failed, both the list
    //  of files we put into the IntermediateFilename last time and any new files
    //  we extract from InputFilename on this run will be merged into a list of
    //  all changed files since the last SUCCESSFUL backup.
    var InputLineReader = new LineByLineReader(InputFilename, {skipEmptyLines: true});

    // If something fails, fail the promise
    InputLineReader.on('error', function(err) {
      console.log(err);
      deferred.fail(err);
    });

    InputLineReader.on('line', function(line) {
      // Pause the generation of "line" events while we process this line
      InputLineReader.pause();

      FileSystem.appendFile(IntermediateFilename, line + '\r\n', function() {
        //TODO: Add a setTimeout to limit CPU usage?
        InputLineReader.resume();
      })
    });

    InputLineReader.on('end', function() {
      // Purge the OutputFilename as anything in it is the past.
      FileSystem.truncate(OutputFilename, 0, function(err) {
        // We're not going to check "err".  It's not really important to act on
        //  the file not existing, etc. and we will handle write errors later.

        // Purge the InputFilename because everything in it has been safely moved
        //  to the IntermediateFilename
        FileSystem.truncate(InputFilename, 0, function(err) {
          if (err) {
            console.log(err);
            deferred.fail(err);
            return;
          }

          FileSystem.stat(IntermediateFilename, function(err, stats) {
            if (err) {
              // If the IntermediateFilename wasn't created, it might be because
              //  there were no new lines in the InputFilename.  This is not an
              //  error.
              if (err.code == 'ENOENT') {
                console.log("No new lines found?");
                deferred.resolve(OutputLines);
              }
              else {
                console.log(err);
                deferred.fail(err);
              }
              return;
            }

            // Now we take the content of IntermediateFilename, reformat it's lines
            //  and output them to the OutputFilename.
            var LineReader = new LineByLineReader(IntermediateFilename, {skipEmptyLines: true});

            // If something fails, fail the promise
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

              // If this line already exists in the OutputLines array, don't process
              //  it again
              if (OutputLines[ThisFileName]) {
                //TODO: Add a setTimeout to limit CPU usage?
                LineReader.resume();
              }

              // If the line is only whitespace, it is junk.
              else if (line.replace(/\s/).length == 0) {
                console.log("Dropping a line of junk: '" + line + "'");
                LineReader.resume();
              }

              // Somehow my test data ended up with a bunch of null characters
              //  which made weird things happen.  This should never happen in
              //  the real world but we'll filter out lines with nulls anyway as
              //  a precaution.
              else if (line.match(/\0/)) {
                console.log("Dropping a line with null characters: '" + line.replace(/\0\0.*\0/g, "\\0\\0...\\0").replace(/\0/g, "\\0") + "'");
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
              });
            });
          });
        });
      });
    });
  });

  return deferred.promise;
}

// Main script entry point
//  This would be simplier if rewritten with generators/yield but I'll get to
//  know promises better first.
let Volumes = {};
let VolumeSnapshots = {};
let VolumeSuitableSnapshots = {};
let BackupResult = "failed";
let BackupCleanup = false;
let BackupType = "incr";
process.argv.forEach(function(val, index, array) {
  if ((val.match(/post-backup/i)) || (val.match(/postbackup/i))) {
    if (index <= array.length) {
      BackupResult = array[index + 1];
    }
    console.log("Post backup cleanup for " + BackupResult + " backup!")
    BackupCleanup = true;
  }
  else if ((val.match(/backup-type/i)) || (val.match(/backuptype/i) || (val.match(/type/i)))) {
    if (index <= array.length) {
      BackupType = array[index + 1];
      console.log("Rsync will be configured for a " + BackupType + " backup.");
    }
  }
  else if ((val.match(/full/i))) {
    BackupType = "full";
    console.log("Rsync will be configured for a " + BackupType + " backup.");
  }
});

let AppSettings = {
  // Cygwin default settings
  'RsyncConfPath': 'C:\\cygwin\\etc\\rsyncd.conf',
  'RsyncLockPath': '/var/run/rsyncd.lock',
  'RsyncLogPath': '/var/log/rsyncd.log',
  'RsyncdServiceName': 'rsyncd',

  // Other default setting values
  'MaxUsableSnapshotAge': 3600
};

// The first async step is loading a list of volumes and an array of settings
//  from the Windows registry.
Q.allSettled([EnumVolumes(), GetSettings()])
.then(function(items) {
  Volumes = items[0].value;

  // Override the HookScripts.SearchPaths setting if one was specified in the
  //  registry.
  if (AppSettings['HookScriptsDir']) {
    HookScripts.SearchPaths = [ AppSettings['HookScriptsDir'] ];
  }

  if (BackupCleanup) {
    return Q.allSettled([HookScripts.RunScripts("post-backup")]);
  }
  else {
    // Execute any pre-snapshot hook scripts
    return Q.allSettled([HookScripts.RunScripts("pre-snapshot")]);
  }
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
  let VolumeActionPromises = [];
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


  if (BackupCleanup) {
    //TODO: Cleanup previously created snapshots
    for (let volumeIndex in Volumes) {
      let volume = Volumes[volumeIndex];
      for (let index in VolumeSnapshots[volumeIndex]) {
        let snapshot = VolumeSnapshots[volumeIndex][index];
        //console.log(snapshot);
        //console.log(volume["SnapshotsCreated"]);
        if (volume["SnapshotsCreated"]) {
          if (volume["SnapshotsCreated"].match(snapshot.ID)) {
            console.log("Snapshot should be deleted.", snapshot);
            VolumeActionPromises.push(snapshot.Delete());
          }

          //TODO: Purge the SnapshotsCreated registry value
        }
      }
    }
  }
  else {
    // If a snapshot was created recently, use it.  Otherwise, create a new
    //  snapshot now
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
      if ((!VolumeSuitableSnapshots[index]) && (!BackupCleanup)) {
        console.log("No suitable existing snapshot of " + index + " exists.  A new snapshot will be created.");
        VolumeActionPromises.push(VSSSnapshots.Create(index));
      }
      else if (!BackupCleanup)  {
        console.log("Using existing snapshot of " + index + " created " + VolumeSuitableSnapshots[index].Age() + "s ago");
      }
    }
  }
  return Q.allSettled(VolumeActionPromises);
})

.then(function(items) {
  let SaveNewSnapshotPromise = '';
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

        //TODO: Record the creation of a snapshot so it can be removed by a
        //  cleanup operation later
        SaveNewSnapshotPromise = SaveNewSnapshot(items[index].value.Volume, items[index].value);
      }
    }
    catch (err) { console.log(err) }
  }

  if (BackupCleanup) {
    // Execute hooks for this specific backup result (success/failed)
    return Q.allSettled([HookScripts.RunScripts("post-backup-" + BackupResult)]);
  }
  else {
    // Execute any post-snapshot hook scripts
    return Q.allSettled([SaveNewSnapshotPromise, HookScripts.RunScripts("post-snapshot")]);
  }
})

.then(function(items) {
  var ProcessFilePromises = [];

  // Extract the OutputToFile values for each volume and start a ProcessFile()
  for (let index in Volumes) {
    var InputFilename = Volumes[index]['OutputToFile'];

    //TODO: Stop the relevant BackupQueueFromUSNJournal services

    // The output file is going to be the same path as the input file only
    //  without the .in at the end.
    //TODO: Make the OutputFilename and IntermediateFilename customisable from
    //  the registry (same key as read by EnumVolumes)
    var OutputFilename = Path.parse(InputFilename);
    OutputFilename.ext = "";
    OutputFilename.base = "";
    OutputFilename = Path.format(OutputFilename);

    var IntermediateFilename = Path.parse(InputFilename);
    IntermediateFilename.ext = ".queue";
    IntermediateFilename.base = "";
    IntermediateFilename = Path.format(IntermediateFilename);

    // We need to remember the OutputFilename for later when we're configuring
    //  the rsync daemon
    Volumes[index]['IncludeFromFile'] = OutputFilename;

    console.log(InputFilename);
    console.log(OutputFilename);

    if (BackupCleanup) {
      //TODO: If the backup was successful, purge the IntermediateFilename
      //FileSystem.truncate(IntermediateFilename, 0, function(err) {
      //
      //}
    }
    else {
      ProcessFilePromises.push(ProcessFile(InputFilename, IntermediateFilename, OutputFilename));
    }
  }

  // Only return from this "then" block once all of the ProcessFile promises are
  //  settled.
  return Q.allSettled(ProcessFilePromises);
})
.then(function() {
  if (BackupCleanup) {
    //TODO: Anything left for the cleanup process?
  }
  else {
    var CyclePromises = [];

    for (let index in Volumes) {
      console.log(Volumes[index]);

      //TODO: Cycle the BackupQueueFromUSNJournal logs
      if (Volumes[index]['LogToFile']) {
        let LogFile = CycleLogs(Volumes[index]['LogToFile'] + '.verbose.log');
        CyclePromises.push(LogFile.cycle());

        LogFile = CycleLogs(Volumes[index]['LogToFile'] + '.errors.log');
        CyclePromises.push(LogFile.cycle());

        LogFile = CycleLogs(Volumes[index]['LogToFile'] + '.log');
        CyclePromises.push(LogFile.cycle());
      }

      //TODO: Start the BackupQueueFromUSNJournal services
    }
    return Q.allSettled(CyclePromises);
  }
})
.then(function() {
  console.log("Done Cycling");
  if (BackupCleanup) {
    //TODO: Anything left for the cleanup process?
  }
  else {
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
          if (BackupType != "full") {
            NewModule.IncludeFrom = Volumes[index]['IncludeFromFile'];
          }
          RsyncConf.AddModule(NewModule);
        }
        else {
          console.log("WARNING: The snapshot selected for " + index + " isn't actaully a snapshot.  This should never happen!");
        }
      }
      else {
        console.log("WARNING: No suitable snapshots of " + index + " were found or created.  This should never happen!");
      }
    }
    //console.log(RsyncConf.toString());

    // Save the config and restart the service (the final argument is the
    //  customised service name)
    return RsyncConf.Save(AppSettings['RsyncConfPath'], AppSettings['RsyncdServiceName']);
  }
})
.then(function() {
  // We're all done?
})
.fail(function(err) {
  console.log("Promise Error: ", err.message);
})
.done();
