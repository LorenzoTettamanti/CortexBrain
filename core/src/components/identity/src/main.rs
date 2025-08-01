/*
 * CortexBrain Identity Service
 * Open Issues: #105 #107
 * Features:
 *   1. TCP, UDP , ICMP events tracker
 *   2. Track Connections using a PerfEventArray named ConnArray
 *   3. Track veth creation and deletion events
 *
 */
#![allow(unused_mut)]
#![allow(warnings)]

mod enums;
mod helpers;
mod structs;
use aya::{
    Bpf,
    maps::{
        Map, MapData,
        perf::{PerfEventArray, PerfEventArrayBuffer},
    },
    programs::{KProbe, SchedClassifier, TcAttachType, tc::SchedClassifierLinkId},
    util::online_cpus,
};
use libc::signal;

use crate::helpers::{display_events, display_veth_events, get_veth_channels};
use bytes::BytesMut;
use std::{
    convert::TryInto,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Error, Ok};
use tokio::{fs, signal};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

const BPF_PATH: &str = "BPF_PATH"; //BPF env path
const PIN_MAP_PATH: &str = "PIN_MAP_PATH";

use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    //init tracing subscriber
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_level(true)
        .with_span_events(FmtSpan::NONE)
        .with_file(false)
        .pretty()
        .with_env_filter(EnvFilter::new("info"))
        .with_line_number(false)
        .init();

    info!("Starting identity service...");
    info!("fetching data");

    // To Store link_ids they can be used to detach tc
    let link_ids = Arc::new(Mutex::new(HashMap::<String, SchedClassifierLinkId>::new()));

    //init conntracker data path
    let bpf_path = std::env::var(BPF_PATH).context("BPF_PATH environment variable required")?;
    let data = fs::read(Path::new(&bpf_path))
        .await
        .context("failed to load file from path")?;

    //init bpf data
    let bpf = Arc::new(Mutex::new(Bpf::load(&data)?));
    let bpf_map_save_path =
        std::env::var(PIN_MAP_PATH).context("BPF_PATH environment variable required")?;

    match init_bpf_maps(bpf.clone()) {
        std::result::Result::Ok(bpf_maps) => {
            info!("Successfully loaded bpf maps");

            //TODO: save the bpf maps in a Vec instead of using a tuple
            match map_pinner(&bpf_maps, &bpf_map_save_path.into()).await {
                std::result::Result::Ok(_) => {
                    info!("maps pinned successfully");
                    //load veth_trace program ref veth_trace.rs
                    init_veth_tracer(bpf.clone()).await?;

                    let interfaces = get_veth_channels();

                    info!("Found interfaces: {:?}", interfaces);
                    init_tc_classifier(bpf.clone(), interfaces, link_ids.clone())
                        .await
                        .context(
                            "An error occured during the execution of attach_bpf_program function",
                        )?;

                    event_listener(bpf_maps, link_ids.clone(), bpf.clone())
                        .await
                        .context("Error initializing event_listener")?;
                }
                Err(e) => {
                    error!("Error while pinning bpf_maps: {}", e);
                    signal::ctrl_c();
                }
            }
        }
        Err(e) => {
            error!("Error while loading bpf maps {}", e);
            signal::ctrl_c();
        }
    }

    Ok(())
}

//attach the tc classifier program to a vector of interfaces
async fn init_tc_classifier(
    bpf: Arc<Mutex<Bpf>>,
    ifaces: Vec<String>,
    link_ids: Arc<Mutex<HashMap<String, SchedClassifierLinkId>>>,
) -> Result<(), anyhow::Error> {
    //this funtion initialize the tc classifier program
    info!("Loading programs");

    let mut bpf_new = bpf.lock().unwrap();

    let program: &mut SchedClassifier = bpf_new
        .program_mut("identity_classifier")
        .ok_or_else(|| anyhow::anyhow!("program 'identity_classifier' not found"))?
        .try_into()
        .context("Failed to init SchedClassifier program")?;

    program
        .load()
        .context("Failed to load identity_classifier program")?;

    for interface in ifaces {
        match program.attach(&interface, TcAttachType::Ingress) {
            std::result::Result::Ok(link_id) => {
                info!(
                    "Program 'identity_classifier' attached to interface {}",
                    interface
                );
                let mut map = link_ids.lock().unwrap();
                map.insert(interface.clone(), link_id);
            }
            Err(e) => error!(
                "Error attaching program to interface {}: {:?}",
                interface, e
            ),
        }
    }

    Ok(())
}

async fn init_veth_tracer(bpf: Arc<Mutex<Bpf>>) -> Result<(), anyhow::Error> {
    //this functions init the veth_tracer used to make the InterfacesRegistry

    let mut bpf_new = bpf.lock().unwrap();

    //creation tracer
    let veth_creation_tracer: &mut KProbe = bpf_new
        .program_mut("veth_creation_trace")
        .ok_or_else(|| anyhow::anyhow!("program 'veth_creation_trace' not found"))?
        .try_into()?;
    veth_creation_tracer.load()?;

    match veth_creation_tracer.attach("register_netdevice", 0) {
        std::result::Result::Ok(_) => info!("veth_creation_tracer program attached successfully"),
        Err(e) => error!("Error attaching veth_creation_tracer program {:?}", e),
    }

    //deletion tracer
    let veth_deletion_tracer: &mut KProbe = bpf_new
        .program_mut("veth_deletion_trace")
        .ok_or_else(|| anyhow::anyhow!("program 'veth_deletion_trace' not found"))?
        .try_into()?;
    veth_deletion_tracer
        .load()
        .context("Failed to load deletetion_tracer program")?;

    match veth_deletion_tracer.attach("unregister_netdevice_queue", 0) {
        std::result::Result::Ok(_) => info!("veth_deletion_trace program attached successfully"),
        Err(e) => error!("Error attaching veth_deletetion_trace program {:?}", e),
    }

    Ok(())
}

fn init_bpf_maps(bpf: Arc<Mutex<Bpf>>) -> Result<(Map, Map), anyhow::Error> {
    // this function init the bpfs maps used in the main program
    /*
       index 0: events_map
       index 1: veth_map
    */
    let mut bpf_new = bpf.lock().unwrap();

    let events_map = bpf_new
        .take_map("EventsMap")
        .ok_or_else(|| anyhow::anyhow!("EventsMap map not found"))?;

    let veth_map = bpf_new
        .take_map("veth_identity_map")
        .ok_or_else(|| anyhow::anyhow!("veth_identity_map map not found"))?;

    /* EDIT: this part is paused right now
       info!("loading bpf connections map");

       //init connection map
       let connections_map_raw = bpf
           .take_map("ConnectionMap")
           .context("failed to take connections map")?;

       let connection_tracker_map = bpf
           .take_map("ConnectionTrackerMap")
           .context("failed to take ConnectionTrackerMap map")?;
    */
    Ok((events_map, veth_map))
}

async fn event_listener(
    bpf_maps: (Map, Map),
    link_ids: Arc<Mutex<HashMap<String, SchedClassifierLinkId>>>,
    bpf: Arc<Mutex<Bpf>>,
) -> Result<(), anyhow::Error> {
    // this function init the event listener. Listens for veth events (creation/deletion) and network events (pod to pod communications)
    /* Doc:

       perf_net_events_array: contains is associated with the network events stored in the events_map (EventsMap)
       perf_veth_array: contains is associated with the network events stored in the veth_map (veth_identity_map)

    */

    info!("Preparing perf_buffers and perf_arrays");

    //TODO: try to change from PerfEventArray to a RingBuffer data structure
    //let m0=bpf_maps[0];
    //let m1 = bpf_maps[1];
    //let mut ring1=RingBuf::try_from(m0)?;
    //let mut ring2=RingBuf::try_from(m1)?;

    //TODO:create an helper function that initialize the data structures and the running
    // init PerfEventArrays
    let mut perf_veth_array: PerfEventArray<MapData> = PerfEventArray::try_from(bpf_maps.1)?;
    let mut perf_net_events_array: PerfEventArray<MapData> = PerfEventArray::try_from(bpf_maps.0)?;
    /*     let mut connections_perf_array = PerCpuHashMap::<&mut MapData,u8,ConnArray>::try_from(connections_map_raw)?; //change with lru hash map*/
    //init PerfEventArrays buffers
    let mut perf_veth_buffer: Vec<PerfEventArrayBuffer<MapData>> = Vec::new();
    let mut perf_net_events_buffer: Vec<PerfEventArrayBuffer<MapData>> = Vec::new();
    /*     let mut connections_perf_buffers = Vec::new(); */

    for cpu_id in online_cpus().map_err(|e| anyhow::anyhow!("Error {:?}", e))? {
        let veth_buf: PerfEventArrayBuffer<MapData> = perf_veth_array.open(cpu_id, None)?;
        perf_veth_buffer.push(veth_buf);
    }
    for cpu_id in online_cpus().map_err(|e| anyhow::anyhow!("Error {:?}", e))? {
        let events_buf: PerfEventArrayBuffer<MapData> = perf_net_events_array.open(cpu_id, None)?;
        perf_net_events_buffer.push(events_buf);
    }
    info!("Listening for events...");

    let veth_running = Arc::new(AtomicBool::new(true));
    let net_events_running = Arc::new(AtomicBool::new(true));

    let mut veth_buffers = vec![BytesMut::with_capacity(1024); 10];
    let mut events_buffers = vec![BytesMut::with_capacity(1024); online_cpus().iter().len()];
    //   let mut connections_buffers = vec![BytesMut::with_capacity(1024); 10];

    let veth_running_signal = veth_running.clone();
    let net_events_running_signal = net_events_running.clone();
    let veth_link_ids = link_ids.clone();

    //display_events(perf_buffers, running, buffers).await;
    let veth_events_displayer = tokio::spawn(async move {
        display_veth_events(
            bpf.clone(),
            perf_veth_buffer,
            veth_running,
            veth_buffers,
            veth_link_ids,
        )
        .await;
    });
    let net_events_displayer = tokio::spawn(async move {
        display_events(perf_net_events_buffer, net_events_running, events_buffers).await;
    });

    tokio::select! {
        result = veth_events_displayer=>{
            match result{
                Err(e)=>error!("veth_event_displayer panicked {:?}",e),
                std::result::Result::Ok(_) => info!("Found new veth_event"),
                }
        }

        result = net_events_displayer=>{
            match result{
                Err(e)=>error!("net_event_displayer panicked {:?}",e),
                std::result::Result::Ok(_)  => info!("Found new net_event"),
            }
        }
        _= signal::ctrl_c()=>{
            info!("Triggered Exiting...");
            veth_running_signal.store(false, Ordering::SeqCst);
            net_events_running_signal.store(false, Ordering::SeqCst);
        }

    }

    Ok(())
}

//TODO: save bpf maps path in the cli metadata
//takes an array of bpf maps and pin them to persiste session data
//TODO: change maps type with a Vec<Map> instead of (Map,Map). This method is only for fast development and it's not optimized

//chmod 700 <path> to setup the permissions to pin maps TODO:add this permission in the CLI
async fn map_pinner(maps: &(Map, Map), path: &PathBuf) -> Result<(), Error> {
    
    //FIXME: add exception for already pinned maps 
    if !path.exists() {
        error!("Pin path {:?} does not exist. Creating it...", path);
        let _ = fs::create_dir_all(path)
            .await
            .map_err(|e| error!("Failed to create directory: {}", e));
    }

    // Costruisci i path completi per le due mappe
    let map1_path = path.join("events_map");
    let map2_path = path.join("veth_map");

    maps.0.pin(&map1_path)?;
    maps.1.pin(&map2_path)?;

    Ok(())
}
