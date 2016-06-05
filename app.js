"use strict"
var LineByLineReader = require('line-by-line');
var FileSystem = require('fs');
var Path = require('path');
var Registry = require('winreg');
var Q = require('q');

// This creates an insance of a "path" object that will format things in Posix
//  format by default
var PosixPath = require('path').posix;

/*
  Fetch a list of volumes that we should process based on the entries in the
  registry that are used by USNJBackupQueue.
*/
function EnumVolumes() {
  var deferred = Q.defer();
  var ReturnKeys = {};

  // Enumerate Volumes and input files that will need to be processed
  var VolumesKey = new Registry({
    hive: Registry.HKLM,
    key: '\\Software\\USNJBackupQueue\\Volumes'
  });

  VolumesKey.keys(function(err, items) {
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
  // Are are going to wedge each file/directory into an object using the full path
  //  as the index.  This is a dodgy, memory consuming way to dedup the list.
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
EnumVolumes()
.then(function(items) {
  //TODO: Execute any hook scripts

  //TODO: Create a VSS snapshot

  // Extract the OutputToFile values for each volume and start a ProcessFile()
  var ProcessFilePromises = [];
  for (var index in items) {
    var InputFilename = items[index]['OutputToFile'];

    // The output file is going to be the same path as the input file only
    //  without the .in at the end.
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
  console.log("This should mean that file processing is complete.");

  //TODO: Configure rsync daemon

  //TODO: Restart rsync daemon
})
.fail(function(err) {
  console.log("Error:", err);
})
.done();
