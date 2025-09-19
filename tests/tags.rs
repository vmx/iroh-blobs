#![cfg(feature = "fs-store")]
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ops::Deref,
};

use bao_tree::{Blake3Hasher, Hasher};
use iroh_blobs::{
    api::{
        self,
        tags::{TagInfo, Tags},
        Store,
    },
    store::{fs::FsStore, mem::MemStore},
    BlobFormat, Hash, HashAndFormat,
};
use n0_future::{Stream, StreamExt};
use testresult::TestResult;

async fn to_vec<T>(stream: impl Stream<Item = api::Result<T>>) -> api::Result<Vec<T>> {
    let res = stream.collect::<Vec<_>>().await;
    res.into_iter().collect::<api::Result<Vec<_>>>()
}

fn expected<H: Hasher>(tags: impl IntoIterator<Item = &'static str>) -> Vec<TagInfo> {
    tags.into_iter()
        .map(|tag| TagInfo::new(tag, Hash::new::<H>(tag)))
        .collect()
}

async fn set<H: Hasher>(tags: &Tags, names: impl IntoIterator<Item = &str>) -> TestResult<()> {
    for name in names {
        tags.set(name, Hash::new::<H>(name)).await?;
    }
    Ok(())
}

async fn tags_smoke<H: Hasher>(tags: &Tags) -> TestResult<()> {
    set::<H>(tags, ["a", "b", "c", "d", "e"]).await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>(["a", "b", "c", "d", "e"]));

    let stream = tags.list_range("b".."d").await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>(["b", "c"]));

    let stream = tags.list_range("b"..).await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>(["b", "c", "d", "e"]));

    let stream = tags.list_range(.."d").await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>(["a", "b", "c"]));

    let stream = tags.list_range(..="d").await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>(["a", "b", "c", "d"]));

    tags.delete_range("b"..).await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>(["a"]));

    tags.delete_range(..="a").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>([]));

    set::<H>(tags, ["a", "aa", "aaa", "aab", "b"]).await?;

    let stream = tags.list_prefix("aa").await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>(["aa", "aaa", "aab"]));

    tags.delete_prefix("aa").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>(["a", "b"]));

    tags.delete_prefix("").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>([]));

    set::<H>(tags, ["a", "b", "c"]).await?;

    assert_eq!(
        tags.get("b").await?,
        Some(TagInfo::new("b", Hash::new::<H>("b")))
    );

    tags.delete("b").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected::<H>(["a", "c"]));

    assert_eq!(tags.get("b").await?, None);

    tags.delete_all().await?;

    tags.set("a", HashAndFormat::hash_seq(Hash::new::<H>("a")))
        .await?;
    tags.set("b", HashAndFormat::raw(Hash::new::<H>("b")))
        .await?;
    let stream = tags.list_hash_seq().await?;
    let res = to_vec(stream).await?;
    assert_eq!(
        res,
        vec![TagInfo {
            name: "a".into(),
            hash: Hash::new::<H>("a"),
            format: BlobFormat::HashSeq,
        }]
    );

    tags.delete_all().await?;
    set::<H>(tags, ["c"]).await?;
    tags.rename("c", "f").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(
        res,
        vec![TagInfo {
            name: "f".into(),
            hash: Hash::new::<H>("c"),
            format: BlobFormat::Raw,
        }]
    );

    let res = tags.rename("y", "z").await;
    assert!(res.is_err());
    Ok(())
}

#[tokio::test]
async fn tags_smoke_mem() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let store = MemStore::<Blake3Hasher>::new();
    tags_smoke::<Blake3Hasher>(store.tags()).await
}

#[tokio::test]
async fn tags_smoke_fs() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let td = tempfile::tempdir()?;
    let store = FsStore::load::<Blake3Hasher>(td.path().join("a")).await?;
    tags_smoke::<Blake3Hasher>(store.tags()).await
}

#[tokio::test]
async fn tags_smoke_fs_rpc() -> TestResult<()> {
    tracing_subscriber::fmt::try_init().ok();
    let unspecified = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let (server, cert) = irpc::util::make_server_endpoint(unspecified)?;
    let client = irpc::util::make_client_endpoint(unspecified, &[cert.as_ref()])?;
    let td = tempfile::tempdir()?;
    let store = FsStore::load::<Blake3Hasher>(td.path().join("a")).await?;
    tokio::spawn(store.deref().clone().listen(server.clone()));
    let api = Store::connect(client, server.local_addr()?);
    tags_smoke::<Blake3Hasher>(api.tags()).await?;
    api.shutdown().await?;
    Ok(())
}
