--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- Name: add(integer, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION add(integer, integer) RETURNS integer
    LANGUAGE c STRICT
    AS '$libdir/add_func', 'add_ab';


ALTER FUNCTION public.add(integer, integer) OWNER TO postgres;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: hash; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE hash (
    a integer,
    b integer,
    c integer,
    d integer
);


ALTER TABLE hash OWNER TO postgres;

--
-- Name: test; Type: TABLE; Schema: public; Owner: kingston; Tablespace: 
--

CREATE TABLE test (
    a0 integer,
    a1 integer,
    a2 integer,
    a3 integer
);


ALTER TABLE test OWNER TO kingston;

--
-- Data for Name: hash; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY hash (a, b, c, d) FROM stdin;
\.


--
-- Data for Name: test; Type: TABLE DATA; Schema: public; Owner: kingston
--

COPY test (a0, a1, a2, a3) FROM stdin;
\N	\N	\N	9
\N	\N	\N	11
\N	\N	\N	23
\N	\N	\N	46
\N	\N	\N	85
\N	9	\N	27
\N	2	\N	39
\N	17	\N	35
\N	24	\N	81
\N	82	\N	54
\N	8	9	15
\N	26	53	17
\N	2	91	54
\N	82	46	24
\N	82	43	38
16	36	48	\N
66	43	22	\N
96	24	30	\N
26	58	69	\N
88	55	26	\N
3	4	10	2
56	13	21	7
6	87	37	29
60	27	34	46
96	45	29	33
\.


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

