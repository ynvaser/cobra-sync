package systems.bdev.cobrasync.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.stream.Stream;

public interface CubeCobraS3FileManifestRepository extends JpaRepository<CubeCobraS3FileManifestEntity, String> {
    Stream<CubeCobraS3FileManifestEntity> streamAllBy();
}
