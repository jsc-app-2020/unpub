import 'package:unpub/unpub.dart';

void main() async {
  final store = MongoStore(
    'mongodb://unpub:vrC1fUHoC82tZFw7@10.224.0.90:27017/unpub?authSource=unpub',
  );

  final versions = await store.getPackageVersionsTest('xetia_core');
  print(versions);
}
