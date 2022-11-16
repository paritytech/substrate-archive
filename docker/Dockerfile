FROM docker.io/paritytech/ci-linux:production as builder
LABEL description="build stage for Substrate Archive. We are creating the binary."

ARG PROFILE=release
WORKDIR /substrate-archive

COPY . /substrate-archive

RUN cargo build --$PROFILE --bin polkadot-archive
RUN ls

# ===== SECOND STAGE ======

FROM docker.io/library/debian:buster
LABEL description="This is the 2nd stage: a very small image where we copy the Polkadot binary."
ARG PROFILE=release
RUN ls
COPY --from=builder /substrate-archive/target/$PROFILE/polkadot-archive /usr/local/bin

RUN apt-get update && \
	apt-get install -y libssl1.1 && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 -U -s /bin/sh -d /polkadot-archive polkadot-archive && \
	mkdir -p /polkadot-archive/.local/share && \
	mkdir /data && \
	chown -R polkadot-archive:polkadot-archive /data && \
	ln -s /data /polkadot-archive/.local/share/polkadot-archive && \
	rm -rf /usr/bin /usr/sbin

USER polkadot-archive
VOLUME ["/data"]

CMD ["/usr/local/bin/polkadot-archive"]

