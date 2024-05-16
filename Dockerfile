FROM apache/spark@sha256:a1dd2487a97fb5e35c5a5b409e830b501a92919029c62f9a559b13c4f5c50f63

# Needed for interacting with Slurm
RUN useradd slurm
RUN apt-get update
RUN apt-get install libmunge-dev libmunge2 munge
ENV PATH="${PATH}:/opt/slurm/bin"
