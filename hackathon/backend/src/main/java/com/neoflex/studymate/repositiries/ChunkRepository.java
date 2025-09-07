package com.neoflex.studymate.repositiries;

import com.neoflex.studymate.entities.Chunk;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ChunkRepository extends JpaRepository<Chunk, Long> {
    @Query("SELECT p FROM Chunk p JOIN p.tags k WHERE LOWER(k.name) IN :tags")
    List<Chunk> findByTagsIgnoreCase(@Param("tags") List<String> tags);
}