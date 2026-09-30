package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
)

type publicationRecord struct {
	Scope                 string `json:"scope"`
	PrivateManifestSHA256 string `json:"private_manifest_sha256"`
	OmittedArtifactCount  int    `json:"omitted_artifact_count"`
	Omissions             string `json:"omissions"`
	Review                string `json:"review"`
}

func exportEvidence(input, output string) error {
	root, err := filepath.EvalSymlinks(input)
	if err != nil {
		return err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	data, err := regularRead(root, "manifest.json")
	if err != nil {
		return err
	}
	var report manifest
	if err := json.Unmarshal(data, &report); err != nil {
		return err
	}
	if report.Publication != nil || report.HistoricalRevision != historicalRevision || report.HistoricalSourceRevision != historicalRevision {
		return errors.New("export requires a private capture with the pinned historical revision")
	}
	selected := map[string]bool{"source-manifest.json": true}
	for _, name := range historical {
		selected["historical/"+name] = true
	}
	for index := range report.Commands {
		command := &report.Commands[index]
		if len(command.Args) > 1 && filepath.Base(command.Args[0]) == "go" {
			switch command.Args[1] {
			case "test", "version", "env":
				selected[command.Stdout] = true
			}
		}
		if !selected[command.Stdout] {
			command.Stdout = ""
		}
		command.Stderr = ""
	}
	contents := make(map[string][]byte)
	var included []sourceFile
	for _, artifact := range report.Artifacts {
		if !selected[artifact.Path] {
			continue
		}
		content, err := regularRead(root, artifact.Path)
		if err != nil {
			return fmt.Errorf("export %s: %w", artifact.Path, err)
		}
		if artifact.Deleted || artifact.SHA256 == "" || digest(content) != artifact.SHA256 {
			return fmt.Errorf("export hash mismatch: %s", artifact.Path)
		}
		if _, exists := contents[artifact.Path]; exists {
			return fmt.Errorf("duplicate export artifact: %s", artifact.Path)
		}
		contents[artifact.Path] = content
		included = append(included, artifact)
	}
	for name := range selected {
		if _, exists := contents[name]; !exists {
			return fmt.Errorf("missing selected export artifact: %s", name)
		}
	}
	if digest(contents["source-manifest.json"]) != report.SourceSHA256 {
		return errors.New("export source manifest hash mismatch")
	}
	for _, command := range report.Commands {
		if command.Stdout != "" && digest(contents[command.Stdout]) != command.StdoutSHA256 {
			return fmt.Errorf("export command stdout hash mismatch: %s", command.Stdout)
		}
	}
	report.Publication = &publicationRecord{
		Scope:                 "compact selected evidence, not a complete reproducibility bundle",
		PrivateManifestSHA256: digest(data),
		OmittedArtifactCount:  len(report.Artifacts) - len(included),
		Omissions:             "Only artifacts listed here are included. source/ archives, binaries, binary patches, controller state, native source copies, stderr and unselected stdout remain private and are omitted. source_files and metadata may reference that private archive; they are provenance, not required local artifact paths. Empty command file paths denote omitted private files; original hashes are retained.",
		Review:                "Allowlisted, not sanitized or approved for publication. Review paths, command arguments, metadata and stdout for private identifiers before copying to docs.",
	}
	report.Artifacts = included
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	out, err := freshDirectory(output)
	if err != nil {
		return err
	}
	worker := &runner{out: out}
	for _, artifact := range included {
		if err := worker.write(artifact.Path, contents[artifact.Path]); err != nil {
			return err
		}
	}
	return worker.write("manifest.json", append(encoded, '\n'))
}
