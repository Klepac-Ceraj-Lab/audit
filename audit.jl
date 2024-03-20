using VKCComputing: audit_update_remote!
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

# get files that didn't match pattern (they mostly don't have S_wells)
aws_nosp = subset(aws_files, "seqprep"=>ByRow(ismissing)).file

delete_aws = let
	del = String[]
	# Get the problematic seqids from the file names
	seqs = Set(filter(startswith("SEQ"), first.(split.(aws_nosp, r"[\._]"))))
	# make a new df that has filename-derrived seqids
	df = transform(aws_files, "file"=>  ByRow(f-> first(split(f, r"[\._]"))) => "seqprep_from_file")
	# take only the ones that are problematic
	subset!(df, "seqprep_from_file" => ByRow(s-> s ∈ seqs))
	gdf = groupby(df, "seqprep_from_file")
	for g in gdf
		# make sure there is exactly one non-missing S_well in the subdf
		swell = only(unique(filter(!ismissing, g.S_well)))
		# ensure that the correct file is uploaded
		@assert all(eachrow(g)) do row
			# don't care about files that were correctly parsed
			!ismissing(row.seqprep) && return true
			# there are some random files leftover from kneaddata that we want to delete
			contains(row.file, "joined.fastq.gz") && return true
			id = row.seqprep_from_file
			filesize = row.size
			# this is what the name should be
			newname = replace(row.file, id=> "$(id)_$swell")
			subdf = subset(g, "file"=> ByRow(==(newname)))
			 
			# make sure the the name actually got changed, and the new name already exists
			size(subdf, 1) == 1 && newname != row.file && only(subdf.size) == filesize  
		end
		append!(del, subset(g, "seqprep"=> ByRow(ismissing)).path)
	end
	del
end

for file in delete_aws[6:end]
	run(`aws s3 rm $file --profile wellesley`)
end



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

(airtable_seq, local_seq, good_files, problem_files) = audit_analysis_files(local_files; base)

@info "Ambiguous S_well: $(count(problem_files.s_well_ambiguity))"
@info "Bad suffix: $(count(problem_files.bad_suffix))"
@info "Bad UID: $(count(problem_files.bad_uid))"

VKCComputing.audit_update_remote!(
	select(airtable_seq, "S_well"=>"s_well", Cols(:)),
	select(local_seq, "S_well"=>"s_well", Cols(:));
	dryrun=true
)

aws_files.source .= "aws"
local_files.source .= "grace"


