import 'package:unpub/unpub.dart' as unpub;

main(List<String> args) async {
  final app = unpub.App(
    metaStore: unpub.MongoStore('mongodb://localhost:27017/dart_pub'),
    packageStore: unpub.FileStore('./unpub-packages'),
  );

  final server = await app.serve('0.0.0.0', 4000);
  print('Serving at http://${server.address.host}:${server.port}');
}
