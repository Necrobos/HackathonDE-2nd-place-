package com.neoflex.studymate.entities;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

@Entity
@Table(name = "chunks", schema = "dm")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Chunk {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "title_name", nullable = false)
    private String titleName;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "id_file")
    private DownloadedFile downloadedFile;

    @Column(name = "source_url", columnDefinition = "TEXT")
    private String url;

    @Column(name = "content", nullable = false, columnDefinition = "TEXT")
    private String content;

    @Column(name = "created_at")
    private ZonedDateTime createdAt;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "chunk_tags",
            schema = "dm",
            joinColumns = @JoinColumn(name = "chunk_id"),
            inverseJoinColumns = @JoinColumn(name = "tag_id")
    )
    private Set<Tag> tags;
}
