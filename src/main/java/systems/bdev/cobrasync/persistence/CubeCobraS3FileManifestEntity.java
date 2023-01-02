package systems.bdev.cobrasync.persistence;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import java.util.Date;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class CubeCobraS3FileManifestEntity {
    @Id
    private String id;
    @Column(name = "update_date")
    private Date updateDate;
    @Column(name = "contents")
    @Lob
    private String contents;
}
