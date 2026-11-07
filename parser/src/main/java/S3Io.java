import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

/** Minimal S3 (Garage) I/O. Config from env: S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY. */
final class S3Io {

    private static final S3Client S3 = S3Client.builder()
            .endpointOverride(URI.create(System.getenv("S3_ENDPOINT")))
            .region(Region.US_EAST_1)  // must match s3_region in garage.toml
            .forcePathStyle(true)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(System.getenv("S3_ACCESS_KEY"), System.getenv("S3_SECRET_KEY"))))
            .build();

    private S3Io() {}

    /** "s3://bucket/key" -> {bucket, key} */
    private static String[] split(String uri) {
        String rest = uri.substring("s3://".length());
        int i = rest.indexOf('/');
        return new String[]{rest.substring(0, i), rest.substring(i + 1)};
    }

    static byte[] get(String uri) {
        var p = split(uri);
        return S3.getObjectAsBytes(GetObjectRequest.builder().bucket(p[0]).key(p[1]).build()).asByteArray();
    }

    /** Uploads every file in dir to s3://bucket/prefix/<filename>. */
    static void putDir(Path dir, String uriPrefix) throws IOException {
        var p = split(uriPrefix.endsWith("/") ? uriPrefix : uriPrefix + "/");
        try (var files = Files.list(dir)) {
            for (Path f : (Iterable<Path>) files::iterator) {
                S3.putObject(PutObjectRequest.builder().bucket(p[0]).key(p[1] + f.getFileName()).build(),
                        RequestBody.fromFile(f));
            }
        }
    }
}
