import 'package:intl/intl.dart';
import 'package:mongo_dart/mongo_dart.dart';
import 'package:pub_semver/pub_semver.dart' as semver;
import 'package:unpub/src/models.dart';

import 'meta_store.dart';

final packageCollection = 'packages';
final statsCollection = 'stats';
final versionCollection = 'package_versions';

class MongoStore extends MetaStore {
  Db db;

  MongoStore(this.db) {
    migrateVersions();
  }

  static SelectorBuilder _selectByName(String? name) => where.eq('name', name);

  Future<UnpubQueryResult> _queryPackagesBySelector(
    SelectorBuilder selector, {
    bool fetchDeps = true,
  }) async {
    final count = await db.collection(packageCollection).count(selector);
    final packages = await db
        .collection(packageCollection)
        .find(selector)
        .map((item) => UnpubPackage.fromJson(item))
        .toList();

    if (fetchDeps) {
      Future<void> appendVersions(UnpubPackage package) async {
        final versions = await _getPackageVersions(package.name);
        package.versions.addAll(versions);
        package.versions.sort((a, b) {
          return semver.Version.prioritize(
              semver.Version.parse(a.version), semver.Version.parse(b.version));
        });
      }

      await Future.wait([
        for (final package in packages) appendVersions(package),
      ]);
    }

    return UnpubQueryResult(count, packages);
  }

  Future<List<UnpubVersion>> _getPackageVersions(String name) async {
    final selector = where.eq('name', name);
    final versions = await db.collection(versionCollection).find(selector).map(
      (event) {
        final version = event['version'];
        return UnpubVersion.fromJson(version);
      },
    ).toList();

    return versions;
  }

  @override
  queryPackage(name) async {
    var json =
        await db.collection(packageCollection).findOne(_selectByName(name));
    if (json == null) return null;

    final package = UnpubPackage.fromJson(json);
    package.versions.addAll(await _getPackageVersions(package.name));
    package.versions.sort((a, b) {
      return semver.Version.prioritize(
          semver.Version.parse(a.version), semver.Version.parse(b.version));
    });

    return package;
  }

  @override
  addVersion(name, version) async {
    await db.collection('$versionCollection').insert({
      'name': name,
      'version': version.toJson(),
    });

    await db.collection(packageCollection).update(
          _selectByName(name),
          modify
              .addToSet('uploaders', version.uploader)
              .setOnInsert('createdAt', version.createdAt)
              .setOnInsert('private', true)
              .setOnInsert('download', 0)
              .set('updatedAt', version.createdAt)
              .set('lastVersion', version.toJson()),
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
    bool fetchDeps = true,
  }) {
    var selector =
        where.sortBy(sort, descending: true).limit(size).skip(page * size);

    if (keyword != null) {
      selector = selector.match('name', '.*$keyword.*');
    }

    if (uploader != null) {
      selector = selector.eq('uploaders', uploader);
    }

    if (dependency != null) {
      selector = selector.raw({
        'lastVersion.pubspec.dependencies.$dependency': {r'$exists': true}
      });
    }

    return _queryPackagesBySelector(
      selector,
      fetchDeps: fetchDeps,
    );
  }

  @override
  Future<void> index() async {
    try {
      await db.collection(versionCollection).createIndex(
        keys: {
          'name': 1,
        },
      );
    } catch (e) {}

    try {
      await db.collection(packageCollection).createIndex(
        keys: {
          'name': 1,
        },
      );
    } catch (e) {}
  }

  @override
  Future<void> migrateVersions() async {
    final packages = await db
        .collection(packageCollection)
        .find()
        .map((event) => UnpubPackage.fromJson(event))
        .toList();

    for (final package in packages) {
      if (package.versions.isNotEmpty) {
        for (final version in package.versions) {
          await db.collection('$versionCollection').insert({
            'name': package.name,
            'version': version.toJson(),
          });
        }

        await db.collection(packageCollection).update(
              _selectByName(package.name),
              modify.unset('versions'),
            );
      }

      final pkg = await queryPackage(package.name);
      await db.collection(packageCollection).update(
            _selectByName(package.name),
            modify.set(
              'lastVersion',
              pkg!.versions.last.toJson(),
            ),
          );
    }
  }
}
