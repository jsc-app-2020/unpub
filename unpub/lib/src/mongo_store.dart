import 'package:intl/intl.dart';
import 'package:mongo_dart/mongo_dart.dart';
import 'package:unpub/src/models.dart';

import 'meta_store.dart';

final packageCollection = 'packages';
final statsCollection = 'stats';
final versionCollection = '_version';

class MongoStore extends MetaStore {
  Db db;

  MongoStore(this.db);

  static SelectorBuilder _selectByName(String? name) => where.eq('name', name);

  Future<UnpubQueryResult> _queryPackagesBySelector(
      SelectorBuilder selector) async {
    final count = await db.collection(packageCollection).count(selector);
    final packages = await db
        .collection(packageCollection)
        .find(selector)
        .map((item) => UnpubPackage.fromJson(item))
        .toList();

    for (final package in packages) {
      try {
        package.versions.addAll(await _getPackageVersions(package.name));
      } catch (e) {}
    }

    return UnpubQueryResult(count, packages);
  }

  Future<List<UnpubVersion>> _getPackageVersions(String name) async {
    final versions = await db
        .collection('${name}$versionCollection')
        .find()
        .map((event) => UnpubVersion.fromJson(event))
        .toList();
    return versions;
  }

  @override
  queryPackage(name) async {
    var json =
        await db.collection(packageCollection).findOne(_selectByName(name));
    if (json == null) return null;

    final package = UnpubPackage.fromJson(json);
    package.versions.addAll(await _getPackageVersions(package.name));
    return package;
  }

  @override
  addVersion(name, version) async {
    await db.collection('$name$versionCollection').insert(version.toJson());
    await db.collection(packageCollection).update(
          _selectByName(name),
          modify
              .addToSet('uploaders', version.uploader)
              .setOnInsert('createdAt', version.createdAt)
              .setOnInsert('private', true)
              .setOnInsert('download', 0)
              .set('updatedAt', version.createdAt),
          upsert: true,
        );
  }

  @override
  addUploader(name, email) async {
    await db
        .collection(packageCollection)
        .update(_selectByName(name), modify.push('uploaders', email));
  }

  @override
  removeUploader(name, email) async {
    await db
        .collection(packageCollection)
        .update(_selectByName(name), modify.pull('uploaders', email));
  }

  @override
  increaseDownloads(name, version) {
    var today = DateFormat('yyyyMMdd').format(DateTime.now());
    db
        .collection(packageCollection)
        .update(_selectByName(name), modify.inc('download', 1));
    db
        .collection(statsCollection)
        .update(_selectByName(name), modify.inc('d$today', 1));
  }

  @override
  Future<UnpubQueryResult> queryPackages({
    required size,
    required page,
    required sort,
    keyword,
    uploader,
    dependency,
  }) {
    var selector =
        where.sortBy(sort, descending: true).limit(size).skip(page * size);

    if (keyword != null) {
      selector = selector.match('name', '.*$keyword.*');
    }
    if (uploader != null) {
      selector = selector.eq('uploaders', uploader);
    }
    // if (dependency != null) {
    //   selector = selector.raw({
    //     'versions': {
    //       r'$elemMatch': {
    //         'pubspec.dependencies.$dependency': {r'$exists': true}
    //       }
    //     }
    //   });
    // }

    return _queryPackagesBySelector(selector);
  }
}
