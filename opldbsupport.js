//   Copyright 2020 IBM Corporation
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

// Import a JAR file with some additional error checking
function OPLImportJAR(jar) {
    var JARfile = IloOplCallJava("java.io.File", "<init>", "(Ljava/lang/String;)V",
                               jar);
    if (!JARfile.canRead()) {
      writeln("Cannot read JAR file " + JARfile.getAbsolutePath());
      // Trigger an error if the JAR file does not exist
      IloOplCallJava("ilog.opl.does.not.exist", "<init>", "(Ljava/lang/String;)V",
                     JARfile.getAbsolutePath());
    }
    IloOplImportJava(jar);
}

// Internal function to register the opldbsupport JAR.
// This makes sure that we import the JAR only once.
function __registerJAR(jar) {
  if ( typeof __registerJAR.done == 'undefined' ) {
    __registerJAR.done = true;
    OPLImportJAR(jar);
  }
}

// Register Excel support.
// PREFIX  is the prefix with which the support is registered.
//         This enables statements PREFIXConnection, PREFIXRead,
//         PREFIXPublish
// JAR     is the path to the opldbsupport.jar
// POI     is the path to the poi installation. The function will pick up
//         and load all JARs that are store there.
function OPLRegisterExcel(prefix, jar, poi) {
  // Import all the JARs from Apache-POI
  var POIdir = IloOplCallJava("java.io.File", "<init>", "(Ljava/lang/String;)V",
                              poi);
  var dirs = new Array(".", "lib", "ooxml-lib");
  for (var d = 0; d < dirs.length; ++d) {
    var dir = IloOplCallJava("java.io.File", "<init>", "(Ljava/io/File;Ljava/lang/String;)V", POIdir, dirs[d]);
    var files = dir.listFiles();
    for (var i = 0; i < files.length; ++i) {
      // We cannot use endWith() because files[i] is not mapped to
      // java.lang.String but to an internal class that 
      var fileName = files[i].getName();
      if ( fileName.lastIndexOf(".jar") == fileName.length - 4 ) {
        writeln("Importing " + files[i].getAbsolutePath());
        IloOplImportJava(files[i].getAbsolutePath());
      }
    }
  }

  __registerJAR(jar);

  IloOplCallJava("ilog.opl.externaldata.excel.ExcelConnection",
                 "register",
                 "(Ljava/lang/String;Lilog/opl/IloOplModel;)V",
                 prefix, thisOplModel);
}

// Register JDBC support
// PREFIX  is the prefix with which the support is registered.
//         This enables statements PREFIXConnection, PREFIXRead,
//         PREFIXPublish
// JAR     is the path to the opldbsupport.jar
function OPLRegisterJDBC(prefix, jar) {
  __registerJAR(jar);

  IloOplCallJava("ilog.opl.externaldata.jdbc.JdbcConnection",
                 "register",
                 "(Ljava/lang/String;Lilog/opl/IloOplModel;)V",
                 prefix, thisOplModel);
}
