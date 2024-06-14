[@@@ocaml.warning "-34-37-69"]

exception FunException of string

let _assert_ b msg =
  if b = false
  then raise (FunException msg)

let _assert_eq a b prefix =
  if a <> b
  then raise (FunException ("a b neq " ^ prefix))

 (* var DEBUG = slices.Contains(os.Args, "--debug") *)

let _debug a =
  (* if DEBUG = false { return } *)

  let args = ["hey"; "you"; "guys"; a] in
  print_endline @@ List.fold_right (^) args ""

let () = print_endline "Hello, World!"

type value = {
    tx_start_id : int;
    tx_end_id : int;
    value : string;
  }

type transaction_state = InProgress | Aborted | Committed

type isolation_level =
  ReadUncommited
| ReadCommitted
| RepeatableRead
| Snapshot
| Serializable

module IntSet = Set.Make(Int)
module StringSet = Set.Make(String)

type transaction = {
    isolation : isolation_level;
    id : int;
    state: transaction_state;
    (* Used only by Repeatable Read and stricter. *)
    inprogress: IntSet.t;
   (*  Used only by Snapshot Isolation and stricter. *)
    writeset: StringSet.t option;
    readset: StringSet.t option;
  }

module Database =
  struct
    type t = {
        default_isolation : isolation_level;
        store : (string, value list) Hashtbl.t;
        transactions : (int, transaction) Hashtbl.t;
        next_transaction_id : int;
      }

    let make () = {
        default_isolation = ReadCommitted;
        store = Hashtbl.create 100;
        transactions = Hashtbl.create 100;
        next_transaction_id = 1;
      }

    let in_progress db =
      Hashtbl.fold
        (fun id tx ids -> if tx.state = InProgress then IntSet.add id ids else ids)
        db.transactions
        IntSet.empty

    let new_transaction db =
      let tx = {
          isolation = db.default_isolation;
          state = InProgress;
          id = db.next_transaction_id;
          inprogress = in_progress db;
          writeset = None;
          readset = None;
        } in
      let db = {db with next_transaction_id = db.next_transaction_id + 1} in
      Hashtbl.add db.transactions tx.id tx;
      (db, tx)



  end
