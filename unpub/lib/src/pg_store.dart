import 'dart:convert';

import 'package:postgres/postgres.dart';
import 'package:unpub/unpub.dart';

class PgStore extends MetaStore {
  final String host;
  final int port;
  final String dbName;
  final String user;
  final String password;

  PgStore({
    required this.host,
    required this.port,
    required this.dbName,
    required this.user,
    required this.password,
  });

  Future<T> withDB<T>(
    Future<T> Function(PostgreSQLConnection db) callback,
  ) async {
    final db = PostgreSQLConnection(
      host,
      port,
      dbName,
      username: user,
      password: password,
      useSSL: false,
    );

    try {
      await db.open();
      final result = await callback(db);
      return result;
    } finally {
      if (!db.isClosed) {
        await db.close();
      }
    }
  }

  @override
  Future<void> addUploader(String name, String email) {
    return withDB((db) async {
      await db.execute(
        'INSERT INTO package_uploaders (name, email) VALUES(@name, @email)',
        substitutionValues: {
          'name': name,
          'email': email,
        },
      );
    });
  }

  @override
  Future<void> addVersion(String name, UnpubVersion version) {
    return withDB((db) async {
      await db.transaction((connection) async {
        await connection.execute(
          'INSERT INTO package_versions (name, version, meta) VALUES(@name, @version, @meta)',
          substitutionValues: {
            'name': name,
            'version': version.version,
            'meta': json.encode(version.toJson()),
          },
        );
      });
    });
  }

  @override
  void increaseDownloads(String name, String version) {
    withDB((db) async {
      await db.execute(
        'INSERT INTO package_version_downloads (name, version, count) VALUES(@name, @version, 1) ON CONFLICT (name) DO UPDATE SET count = EXCLUDED.count + 1 WHERE name = @name',
        substitutionValues: {
          'name': name,
          'version': version,
        },
      );
    });
  }

  @override
  Future<void> index() async {}

  @override
  Future<void> migrateVersions() async {}

  @override
  Future<UnpubPackage?> queryPackage(
    String name, {
    bool versionsSortAsc = true,
  }) {
    // TODO: implement queryPackage
    throw UnimplementedError();
  }

  @override
  Future<UnpubPackage?> queryPackageOnly(String name) {
    // TODO: implement queryPackageOnly
    throw UnimplementedError();
  }

  @override
  Future<UnpubPackage?> queryPackageVersion(String name, String version) {
    // TODO: implement queryPackageVersion
    throw UnimplementedError();
  }

  @override
  Future<UnpubQueryResult> queryPackages(
      {required int size,
      required int page,
      required String sort,
      String? keyword,
      String? uploader,
      String? dependency,
      bool fetchDeps = true}) {
    // TODO: implement queryPackages
    throw UnimplementedError();
  }

  @override
  Future<void> removeUploader(String name, String email) {
    // TODO: implement removeUploader
    throw UnimplementedError();
  }
}
