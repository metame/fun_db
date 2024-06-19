[@@@ocaml.warning "-27-34-37-69"]

let ( let* ) = Result.bind

let assert_eq a b prefix =
  if a = b
  then Ok(true)
  else Error("a b neq " ^ prefix)

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

    let complete_transaction db tx state =
      (* debug("completing transaction ", t.id) *)
      let tx = {tx with state = state} in
      Hashtbl.replace db.transactions tx.id tx

    let transaction_state db tx_id =
      let opt_tx = Hashtbl.find_opt db.transactions tx_id in
      Option.to_result ~none:"invalid transaction" opt_tx

    let assert_valid_transaction db tx_opt =
      let* tx = Option.to_result ~none:"transaction is None" tx_opt in
      let* _ = if tx.id > 0 then Ok(true) else Error("invalid id") in
      let* tx = transaction_state db tx.id in
      match tx.state with
      | InProgress -> Ok tx
      | _ -> Error "transaction not in progress"

    let is_visible _v = true

  end

module Connection =
  struct
    type t = {
        tx : transaction option;
        db : Database.t;
      }

    let make db = {
        db;
        tx = None;
      }

    let begin_tx c =
      let* _ = assert_eq c.tx None "no running transactions" in
      let (db, tx) = Database.new_transaction c.db in
      let* tx = Database.assert_valid_transaction db (Some tx) in
      Ok(({db; tx = Some tx}, Int.to_string tx.id))

    let abort_tx c =
      let* tx = Database.assert_valid_transaction c.db c.tx in
      Database.complete_transaction c.db tx Aborted;
      Ok({c with tx = Some tx}, "")

    let commit_tx c =
      let* tx = Database.assert_valid_transaction c.db c.tx in
      Database.complete_transaction c.db tx Committed;
      Ok({c with tx = None}, "")

    let get c args =
      let* tx = Database.assert_valid_transaction c.db c.tx in
      let* key = Option.to_result ~none:"no key provided" (List.nth_opt args 0) in
      let readset = StringSet.add key (Option.get tx.readset) in
      let vs = Hashtbl.find_opt c.db.store key in
      let* vs = Option.to_result ~none:"no value for key exists" vs in
      (* debug(value, c.tx, c.db.isvisible(c.tx, value)) *)
      let v = List.find_opt Database.is_visible vs in
      let v = Option.map (fun v -> v.value) v in
      let v = Option.value ~default:"" v in
      (* TODO: replace transaction in db.transactions *)
      Ok({c with tx = Some({tx with readset = Some(readset)})}, v)

    let invalidate_versions tx_id vs =
      List.fold_left (fun (l, f) v -> if Database.is_visible v
                                      then ({v with tx_end_id = tx_id} :: l, true)
                                      else (v :: l, f))
        ([], false)
        vs

    let set c args =
      let* tx = Database.assert_valid_transaction c.db c.tx in
      let* key = Option.to_result ~none:"no key provided" (List.nth_opt args 0) in
      let vs = Hashtbl.find_opt c.db.store key in
      let* vs = Option.to_result ~none:"no value for key exists" vs in
      let (vs, _) = invalidate_versions tx.id vs in
      let writeset = StringSet.add key (Option.get tx.writeset) in
      let* value = Option.to_result ~none:"no val provided" (List.nth_opt args 1) in
      let value = {tx_start_id = tx.id; tx_end_id = 0; value} in
      Hashtbl.replace c.db.store key (value :: vs);
      (* TODO: replace transaction in db.transactions *)
      Ok({c with tx = Some({tx with writeset = Some(writeset)})}, value.value)


    let exec_command c command args =
      (* debug(command, args) *)
      match command with
      | "begin" -> begin_tx c
      | "abort" -> abort_tx c
      | "commit" -> commit_tx c
      | "get" -> get c args
      | "set" -> set c args
      | _ -> Error "unimplemented"

    let must_exec_command c command args =
      let r = exec_command c command args in
      match r with
      | Ok(r) -> Ok r
      | Error(e) -> Error ("unexpected error " ^ e)

  end
