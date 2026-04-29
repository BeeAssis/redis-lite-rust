use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use std::sync::mpsc::{channel, Sender, Receiver};

enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>)
}

pub enum BlpopResult {
    Popped(Vec<u8>),
    Waiting(Receiver<Vec<u8>>),
}


struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

struct Inner {
    map: HashMap<Vec<u8>, Entry>,
    waiters: HashMap<Vec<u8>, VecDeque<Sender<Vec<u8>>>>,
}

pub struct Store {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
pub enum StoreError{
    WrongType,
}

impl Store {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                waiters: HashMap::new(),
            }),
        }
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl:Option<Duration>) {

        let expires_at = ttl.map(|d| Instant::now() + d);

        // .lock() asks for access of protected data
        let mut inner = self.inner.lock().unwrap();

        inner.map.insert(
            key,
            Entry{
                value: Value::String(value),
                expires_at
            });
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>,StoreError> {
        let mut inner = self.inner.lock().unwrap();

        let expired = match inner.map.get(key){
            Some(entry) => match entry.expires_at{
                Some(deadline) => Instant::now() >= deadline,
                None => false,
            },
            None => return Ok(None)
        };

        if expired {
            inner.map.remove(key);
            return Ok(None);
        }

       match &inner.map.get(key).unwrap().value {
        Value::String(bytes) => Ok(Some(bytes.clone())),
        Value::List(_) => Err(StoreError::WrongType)
       }
    }

    pub fn rpush(&self, key : Vec<u8>, value:Vec<Vec<u8>>)->Result<usize,StoreError>{
      let mut inner = self.inner.lock().unwrap();

       let new_len = match inner.map.get_mut(&key){
            Some(entry) => match &mut entry.value{
                Value::List(list) =>{
                    list.extend(value);
                    list.len()
                }
                Value::String(_) =>{
                    return Err(StoreError::WrongType)

                }          
            },

            None =>{
                let mut list = VecDeque::new();
                list.extend(value);
                let len = list.len();
                inner.map.insert(key.clone(),Entry{ value: Value::List(list), expires_at: None});
                len

            }
           
        };

        loop{


            let waiter_queue = match inner.waiters.get_mut(&key){
                Some(q) => q,
                None => break,
            };

            let tx = match waiter_queue.pop_front(){
                Some(tx) => tx,
                None => break,
            };

            let list = match inner.map.get_mut(&key){
                Some(entry) => match &mut entry.value{
                    Value::List(list) => list,
                    _ => break,
                },
                None => break,
            };

            let elem = match list.pop_front(){
                Some(e) => e,
                None => {
                    inner.waiters.get_mut(&key).unwrap().push_front(tx);
                    break;
                }
            };

            let _ = tx.send(elem);


        }

        Ok(new_len)


    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>, StoreError>{
        let inner = self.inner.lock().unwrap();


         match inner.map.get(key){
             Some(entry) => match &entry.value{
                Value::List(list) =>{
                    let len = list.len() as i64;
                    if len == 0 {
                        return Ok(Vec::new());
                    }

                    let mut start = start;
                    let mut stop = stop;

                    

                    if start < 0 {
                        start = start + len;

                    }

                    if stop < 0{
                        stop = stop + len;
                    }

                    if start < 0{
                        start = 0;
                    }

                    if stop >= len{
                        stop = len - 1;
                    }

                    if stop < start{
                        return Ok(Vec::new()); 

                    }

                    let start_usize = start as usize;
                    let count_usize = (stop - start + 1) as usize;


                    let result: Vec<Vec<u8>> = list
                        .iter()
                        .skip(start_usize)
                        .take(count_usize)
                        .cloned()
                        .collect();

                    Ok(result)
        
                 }
                Value::String(_) => Err(StoreError::WrongType),
             
             },
               None =>Ok(Vec::new())
            }

         }


    pub fn lpush(&self, key : Vec<u8>, value:Vec<Vec<u8>>)->Result<usize,StoreError>{
      let mut inner = self.inner.lock().unwrap();

       let new_len = match inner.map.get_mut(&key){
            Some(entry) => match &mut entry.value{
                Value::List(list) =>{
                    for v in value{
                        list.push_front(v);
                    }
          
                    list.len()
                }
                Value::String(_) =>{
                    return Err(StoreError::WrongType)

                }          
            },

            None =>{
                let mut list = VecDeque::new();

                for v in value{
                    list.push_front(v);

                }
                let len = list.len();
                inner.map.insert(key.clone(),Entry{ value: Value::List(list), expires_at: None});
                len

            }
           
        };

         loop{


            let waiter_queue = match inner.waiters.get_mut(&key){
                Some(q) => q,
                None => break,
            };

            let tx = match waiter_queue.pop_front(){
                Some(tx) => tx,
                None => break,
            };

            let list = match inner.map.get_mut(&key){
                Some(entry) => match &mut entry.value{
                    Value::List(list) => list,
                    _ => break,
                },
                None => break,
            };

            let elem = match list.pop_front(){
                Some(e) => e,
                None => {
                    inner.waiters.get_mut(&key).unwrap().push_front(tx);
                    break;
                }
            };

            let _ = tx.send(elem);


        }

        Ok(new_len)

    }

    pub fn llen(&self, key: &[u8]) -> Result<usize,StoreError> {
        let  inner = self.inner.lock().unwrap();
        
        match inner.map.get(key) {
            Some(entry) => match &entry.value{
                Value::List(list) => Ok(list.len()),
                Value::String(_) => Err(StoreError::WrongType),
            },
            None => Ok(0)
       
       }
    }

    pub fn lpop(&self, key: &[u8] )-> Result<Option<Vec<u8>>, StoreError>{
      let mut inner = self.inner.lock().unwrap();

       match inner.map.get_mut(key){
            Some(entry) => match &mut entry.value{
                Value::List(list) =>Ok(list.pop_front()),
                Value::String(_) => Err(StoreError::WrongType)           
            },
            None => Ok(None)
           
        }
    }

    pub fn lpop_count(&self, key: &[u8],count: usize )-> Result<Vec<Vec<u8>>, StoreError>{
      let mut inner = self.inner.lock().unwrap();

       match inner.map.get_mut(key){
            Some(entry) => match &mut entry.value{
                Value::List(list) =>{
                    let mut result = Vec::new();
                    for _ in 0..count{
                        match list.pop_front(){
                            Some(v) => result.push(v),
                            None => break,
                        }
                       
                    }
                    Ok(result)
               
                },
                Value::String(_) => Err(StoreError::WrongType)           
            },
            None => Ok(Vec::new()),
           
        }
    }

    pub fn blpop(&self, key : Vec<u8>) -> Result<BlpopResult,StoreError>{
         let mut inner = self.inner.lock().unwrap();
         match inner.map.get_mut(&key){
            Some(entry) => match &mut entry.value{
                Value::List(list) => {
                    if !list.is_empty(){
                        let elem = list.pop_front().unwrap();
                        return Ok(BlpopResult::Popped(elem));
                    }
                  
                }
                 Value::String(_) => {
                    return Err(StoreError::WrongType);
                }
            },
            None =>{

            },
         }
        let (tx, rx) = channel();
        inner.waiters
            .entry(key)
            .or_insert_with(VecDeque::new)
            .push_back(tx);
        Ok(BlpopResult::Waiting(rx))
    }
   

}



