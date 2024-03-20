# # VKC Lab sequencing files audit
#
# Checking to make sure we have all expected files,
# and all are where they are supposed to be.

# ## Checking Databases

using VKCComputing
using DataFrames
using Chain
using PrettyTables

base = LocalBase(; update=true)

bios = biospecimens(base; strict=false)
sps = seqpreps(base; strict=false)

@info "Unsequenced seqpreps: $(subset(sps, "sequencing_batch"=> ByRow(ismissing)).uid)"

subset!(sps, "sequencing_batch"=> ByRow(!ismissing))

@assert all(rec-> length(rec) == 1, sps.sequencing_batch)
@assert all(rec-> length(rec) == 1, sps.project)
@assert all(rec-> length(rec) == 1, sps."subject (from biospecimen)")
@assert all(rec-> length(rec) == 1, sps.biospecimen_keep)

sps.sequencing_batch = [rec.fields[:uid] for rec in resolve_links(base, sps.sequencing_batch)]
sps.project = [rec.fields[:uid] for rec in resolve_links(base, sps.project)]
sps."subject (from biospecimen)" = [rec.fields[:uid] for rec in resolve_links(base, sps."subject (from biospecimen)")]
sps.biospecimen_keep = Bool.(first.(sps.biospecimen_keep))
sps.keep = Bool.(sps.keep)

aws_files = aws_ls("s3://wc-vanja-klepac-ceraj/sequencing/processed/mgx/"; profile="wellesley")
local_files = get_analysis_files()

aws_nosp = subset(aws_files, "seqprep"=>ByRow(ismissing)).file
local_nosp = subset(local_files, "seqprep"=>ByRow(ismissing)).file

let seqs = filter(startswith("SEQ"), local_nosp)
	for seq in seqs
		m = match(r"(SEQ\d+)", seq)
		
		df = subset(local_files, "file"=> ByRow(startswith(m.match)))
		swell = only(unique(filter(!ismissing, df.s_well)))
		subset!(df, "s_well"=>ByRow(ismissing))
		for row in eachrow(df)
			newfile = replace(row.file, m.match=> "$(string(m.match, "_", swell))")
			@info "renaming $(row.file) to $newfile"
			mv(row.path, joinpath(row.dir, newfile))
		end
	end
end
