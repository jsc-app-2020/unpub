import 'package:path/path.dart' as path;
import 'package:unpub/unpub.dart' as unpub;

Future<void> main() async {
  // var parser = ArgParser();
  // parser.addOption('host', abbr: 'h', defaultsTo: '0.0.0.0');
  // parser.addOption('port', abbr: 'p', defaultsTo: '4000');
  // parser.addOption('database',
  //     abbr: 'd', defaultsTo: 'mongodb://localhost:27017/dart_pub');

  // var results = parser.parse(args);

  var host = '0.0.0.0';
  var port = 8080;
  var dbUri = 'mongodb://admin:secret@localhost:27017/app?authSource=admin';

  var baseDir = path.absolute('unpub-packages');

  var app = unpub.App(
    metaStore: unpub.MongoStore(dbUri),
    packageStore: unpub.FileStore(baseDir),
    overrideUploaderEmail: 'mr.poetra22@gmail.com',
  );

  var server = await app.serve(host, port);
  print('Serving at http://${server.address.host}:${server.port}');
}
